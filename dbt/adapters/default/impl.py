import copy
import multiprocessing
import time
import agate

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags
import dbt.schema
import dbt.clients.agate_helper

from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Column
from dbt.utils import filter_null_values

from dbt.adapters.default.relation import DefaultRelation

GET_CATALOG_OPERATION_NAME = 'get_catalog_data'

lock = multiprocessing.Lock()
connections_in_use = {}
connections_available = []


class DefaultAdapter(object):
    DEFAULT_QUOTE = True

    requires = {}

    context_functions = [
        "get_columns_in_table",
        "get_missing_columns",
        "expand_target_column_types",
        "create_schema",
        "quote_as_configured",

        # deprecated -- use versions that take relations instead
        "already_exists",
        "query_for_existing",
        "rename",
        "drop",
        "truncate",

        # just deprecated. going away in a future release
        "quote_schema_and_table",

        # versions of adapter functions that take / return Relations
        "list_relations",
        "get_relation",
        "drop_relation",
        "rename_relation",
        "truncate_relation",
    ]

    profile_functions = [
        "execute",
        "add_query",
    ]

    raw_functions = [
        "get_status",
        "get_result_from_cursor",
        "quote",
        "convert_type"
    ]

    Relation = DefaultRelation
    Column = Column

    ###
    # ADAPTER-SPECIFIC FUNCTIONS -- each of these must be overridden in
    #                               every adapter
    ###
    @classmethod
    @contextmanager
    def exception_handler(cls, config, sql, model_name=None,
                          connection_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`exception_handler` is not implemented for this adapter!')

    @classmethod
    def type(cls):
        raise dbt.exceptions.NotImplementedException(
            '`type` is not implemented for this adapter!')

    @classmethod
    def date_function(cls):
        raise dbt.exceptions.NotImplementedException(
            '`date_function` is not implemented for this adapter!')

    @classmethod
    def get_status(cls, cursor):
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!')

    @classmethod
    def alter_column_type(cls, config, schema, table,
                          column_name, new_column_type, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`alter_column_type` is not implemented for this adapter!')

    @classmethod
    def query_for_existing(cls, config, schemas,
                           model_name=None):
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        all_relations = []

        for schema in schemas:
            all_relations.extend(
                cls.list_relations(config, schema, model_name))

        return {relation.identifier: relation.type
                for relation in all_relations}

    @classmethod
    def get_existing_schemas(cls, config, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`get_existing_schemas` is not implemented for this adapter!')

    @classmethod
    def check_schema_exists(cls, config, schema):
        raise dbt.exceptions.NotImplementedException(
            '`check_schema_exists` is not implemented for this adapter!')

    @classmethod
    def cancel_connection(cls, config, connection):
        raise dbt.exceptions.NotImplementedException(
            '`cancel_connection` is not implemented for this adapter!')

    ###
    # FUNCTIONS THAT SHOULD BE ABSTRACT
    ###
    @classmethod
    def get_result_from_cursor(cls, cursor):
        data = []
        column_names = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            raw_results = cursor.fetchall()
            data = [dict(zip(column_names, row))
                    for row in raw_results]

        return dbt.clients.agate_helper.table_from_data(data, column_names)

    @classmethod
    def drop(cls, config, schema, relation, relation_type, model_name=None):
        identifier = relation
        relation = cls.Relation.create(
            schema=schema,
            identifier=identifier,
            type=relation_type)

        return cls.drop_relation(config, relation, model_name)

    @classmethod
    def drop_relation(cls, config, relation, model_name=None):
        if relation.type is None:
            dbt.exceptions.raise_compiler_error(
                'Tried to drop relation {}, but its type is null.'
                .format(relation))

        sql = 'drop {} if exists {} cascade'.format(relation.type, relation)

        connection, cursor = cls.add_query(config, sql, model_name,
                                           auto_begin=False)

    @classmethod
    def truncate(cls, config, schema, table, model_name=None):
        relation = cls.Relation.create(
            schema=schema,
            identifier=table,
            type='table')

        return cls.truncate_relation(config, relation, model_name)

    @classmethod
    def truncate_relation(cls, config,
                          relation, model_name=None):
        sql = 'truncate table {}'.format(relation)

        connection, cursor = cls.add_query(config, sql, model_name)

    @classmethod
    def rename(cls, config, schema,
               from_name, to_name, model_name=None):
        return cls.rename_relation(
            config,
            from_relation=cls.Relation.create(
                schema=schema, identifier=from_name),
            to_relation=cls.Relation.create(
                identifier=to_name),
            model_name=model_name)

    @classmethod
    def rename_relation(cls, config, from_relation, to_relation,
                        model_name=None):
        sql = 'alter table {} rename to {}'.format(
            from_relation, to_relation.include(schema=False))

        connection, cursor = cls.add_query(config, sql, model_name)

    @classmethod
    def is_cancelable(cls):
        return True

    @classmethod
    def get_missing_columns(cls, config,
                            from_schema, from_table,
                            to_schema, to_table,
                            model_name=None):
        """Returns dict of {column:type} for columns in from_table that are
        missing from to_table"""
        from_columns = {col.name: col for col in
                        cls.get_columns_in_table(
                            config, from_schema, from_table,
                            model_name=model_name)}
        to_columns = {col.name: col for col in
                      cls.get_columns_in_table(
                          config, to_schema, to_table,
                          model_name=model_name)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items()
                if col_name in missing_columns]

    @classmethod
    def _get_columns_in_table_sql(cls, schema_name, table_name, database):
        schema_filter = '1=1'
        if schema_name is not None:
            schema_filter = "table_schema = '{}'".format(schema_name)

        db_prefix = '' if database is None else '{}.'.format(database)

        sql = """
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision || ',' || numeric_scale as numeric_size

        from {db_prefix}information_schema.columns
        where table_name = '{table_name}'
          and {schema_filter}
        order by ordinal_position
        """.format(db_prefix=db_prefix,
                   table_name=table_name,
                   schema_filter=schema_filter).strip()

        return sql

    @classmethod
    def get_columns_in_table(cls, config, schema_name,
                             table_name, database=None, model_name=None):
        sql = cls._get_columns_in_table_sql(schema_name, table_name, database)
        connection, cursor = cls.add_query(config, sql, model_name)

        data = cursor.fetchall()
        columns = []

        for row in data:
            name, data_type, char_size, numeric_size = row
            column = cls.Column(name, data_type, char_size, numeric_size)
            columns.append(column)

        return columns

    @classmethod
    def _table_columns_to_dict(cls, columns):
        return {col.name: col for col in columns}

    @classmethod
    def expand_target_column_types(cls, config,
                                   temp_table,
                                   to_schema, to_table,
                                   model_name=None):

        reference_columns = cls._table_columns_to_dict(
            cls.get_columns_in_table(
                config, None, temp_table, model_name=model_name))

        target_columns = cls._table_columns_to_dict(
            cls.get_columns_in_table(
                config, to_schema, to_table,
                model_name=model_name))

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                col_string_size = reference_column.string_size()
                new_type = cls.Column.string_type(col_string_size)
                logger.debug("Changing col type from %s to %s in table %s.%s",
                             target_column.data_type,
                             new_type,
                             to_schema,
                             to_table)

                cls.alter_column_type(config, to_schema,
                                      to_table, column_name, new_type,
                                      model_name)

    ###
    # RELATIONS
    ###
    @classmethod
    def list_relations(cls, config, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`list_relations` is not implemented for this adapter!')

    @classmethod
    def _make_match_kwargs(cls, config, schema, identifier):
        if identifier is not None and config.quoting['identifier'] is False:
            identifier = identifier.lower()

        if schema is not None and config.quoting['schema'] is False:
            schema = schema.lower()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema})

    @classmethod
    def get_relation(cls, config, schema=None, identifier=None,
                     relations_list=None, model_name=None):
        if schema is None and relations_list is None:
            raise dbt.exceptions.RuntimeException(
                'get_relation needs either a schema to query, or a list '
                'of relations to use')

        if relations_list is None:
            relations_list = cls.list_relations(config, schema, model_name)

        matches = []

        search = cls._make_match_kwargs(config, schema, identifier)

        for relation in relations_list:
            if relation.matches(**search):
                matches.append(relation)

        if len(matches) > 1:
            dbt.exceptions.get_relation_returned_multiple_results(
                {'identifier': identifier, 'schema': schema}, matches)

        elif matches:
            return matches[0]

        return None

    ###
    # SANE ANSI SQL DEFAULTS
    ###
    @classmethod
    def get_create_schema_sql(cls, config, schema):
        schema = cls._quote_as_configured(config, schema, 'schema')

        return ('create schema if not exists {schema}'
                .format(schema=schema))

    @classmethod
    def get_drop_schema_sql(cls, config, schema):
        schema = cls._quote_as_configured(config, schema, 'schema')

        return ('drop schema if exists {schema} cascade'
                .format(schema=schema))

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    @classmethod
    def get_default_schema(cls, config):
        return config.credentials.schema

    @classmethod
    def get_connection(cls, config, name=None, recache_if_missing=True):
        global connections_in_use

        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            name = 'master'

        if connections_in_use.get(name):
            return connections_in_use.get(name)

        if not recache_if_missing:
            raise dbt.exceptions.InternalException(
                'Tried to get a connection "{}" which does not exist '
                '(recache_if_missing is off).'.format(name))

        logger.debug('Acquiring new {} connection "{}".'
                     .format(cls.type(), name))

        connection = cls.acquire_connection(config, name)
        connections_in_use[name] = connection

        return cls.get_connection(config, name)

    @classmethod
    def cancel_open_connections(cls, config):
        global connections_in_use

        for name, connection in connections_in_use.items():
            if name == 'master':
                continue

            cls.cancel_connection(config, connection)
            yield name

    @classmethod
    def total_connections_allocated(cls):
        global connections_in_use, connections_available

        return len(connections_in_use) + len(connections_available)

    @classmethod
    def acquire_connection(cls, config, name):
        global connections_available, lock

        # we add a magic number, 2 because there are overhead connections,
        # one for pre- and post-run hooks and other misc operations that occur
        # before the run starts, and one for integration tests.
        max_connections = config.threads + 2

        with lock:
            num_allocated = cls.total_connections_allocated()

            if len(connections_available) > 0:
                logger.debug('Re-using an available connection from the pool.')
                to_return = connections_available.pop()
                to_return.name = name
                return to_return

            elif num_allocated >= max_connections:
                raise dbt.exceptions.InternalException(
                    'Tried to request a new connection "{}" but '
                    'the maximum number of connections are already '
                    'allocated!'.format(name))

            logger.debug('Opening a new connection ({} currently allocated)'
                         .format(num_allocated))

            result = Connection(
                type=cls.type(),
                name=name,
                state='init',
                transaction_open=False,
                handle=None,
                credentials=config.credentials
            )

            return cls.open_connection(result)

    @classmethod
    def release_connection(cls, config, name='master'):
        global connections_in_use, connections_available, lock

        if name not in connections_in_use:
            return

        to_release = cls.get_connection(config, name, recache_if_missing=False)

        try:
            lock.acquire()

            if to_release.state == 'open':

                if to_release.transaction_open is True:
                    cls.rollback(to_release)

                to_release.name = None
                connections_available.append(to_release)
            else:
                cls.close(to_release)

            del connections_in_use[name]
        finally:
            lock.release()

    @classmethod
    def cleanup_connections(cls):
        global connections_in_use, connections_available, lock

        with lock:
            for name, connection in connections_in_use.items():
                if connection.get('state') != 'closed':
                    logger.debug("Connection '{}' was left open."
                                 .format(name))
                else:
                    logger.debug("Connection '{}' was properly closed."
                                 .format(name))

            conns_in_use = list(connections_in_use.values())
            for conn in conns_in_use + connections_available:
                cls.close(conn)

            # garbage collect these connections
            connections_in_use = {}
            connections_available = []

    @classmethod
    def reload(cls, connection):
        return cls.get_connection(connection.credentials,
                                  connection.name)

    @classmethod
    def add_begin_query(cls, config, name):
        return cls.add_query(config, 'BEGIN', name, auto_begin=False)

    @classmethod
    def add_commit_query(cls, config, name):
        return cls.add_query(config, 'COMMIT', name, auto_begin=False)

    @classmethod
    def begin(cls, config, name='master'):
        global connections_in_use
        connection = cls.get_connection(config, name)

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.get('name')))

        cls.add_begin_query(config, name)

        connection.transaction_open = True
        connections_in_use[name] = connection

        return connection

    @classmethod
    def commit_if_has_connection(cls, config, name):
        global connections_in_use

        if name is None:
            name = 'master'

        if connections_in_use.get(name) is None:
            return

        connection = cls.get_connection(config, name, False)

        return cls.commit(config, connection)

    @classmethod
    def commit(cls, config, connection):
        global connections_in_use

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        connection = cls.reload(connection)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: COMMIT'.format(connection.name))
        cls.add_commit_query(config, connection.name)

        connection.transaction_open = False
        connections_in_use[connection.name] = connection

        return connection

    @classmethod
    def rollback(cls, connection):
        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        connection = cls.reload(connection)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to rollback transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: ROLLBACK'.format(connection.name))
        connection.handle.rollback()

        connection.transaction_open = False
        connections_in_use[connection.name] = connection

        return connection

    @classmethod
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, 'close'):
            connection.handle.close()

        connection.state = 'closed'

        return connection

    @classmethod
    def add_query(cls, config, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):
        connection = cls.get_connection(config, model_name)
        connection_name = connection.name

        if auto_begin and connection.transaction_open is False:
            cls.begin(config, connection_name)

        logger.debug('Using {} connection "{}".'
                     .format(cls.type(), connection_name))

        with cls.exception_handler(config, sql, model_name, connection_name):
            if abridge_sql_log:
                logger.debug('On %s: %s....', connection_name, sql[0:512])
            else:
                logger.debug('On %s: %s', connection_name, sql)
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)

            logger.debug("SQL status: %s in %0.2f seconds",
                         cls.get_status(cursor), (time.time() - pre))

            return connection, cursor

    @classmethod
    def clear_transaction(cls, config, conn_name='master'):
        conn = cls.begin(config, conn_name)
        cls.commit(config, conn)
        return conn_name

    @classmethod
    def execute_one(cls, config, sql, model_name=None, auto_begin=False):
        cls.get_connection(config, model_name)

        return cls.add_query(config, sql, model_name, auto_begin)

    @classmethod
    def execute_and_fetch(cls, config, sql, model_name=None,
                          auto_begin=False):
        _, cursor = cls.execute_one(config, sql, model_name, auto_begin)

        status = cls.get_status(cursor)
        table = cls.get_result_from_cursor(cursor)
        return status, table

    @classmethod
    def execute(cls, config, sql, model_name=None, auto_begin=False,
                fetch=False):
        if fetch:
            return cls.execute_and_fetch(config, sql, model_name, auto_begin)
        else:
            _, cursor = cls.execute_one(config, sql, model_name, auto_begin)
            status = cls.get_status(cursor)
            return status, dbt.clients.agate_helper.empty_table()

    @classmethod
    def execute_all(cls, config, sqls, model_name=None):
        connection = cls.get_connection(config, model_name)

        if len(sqls) == 0:
            return connection

        for i, sql in enumerate(sqls):
            connection, _ = cls.add_query(config, sql, model_name)

        return connection

    @classmethod
    def create_schema(cls, config, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)
        sql = cls.get_create_schema_sql(config, schema)
        res = cls.add_query(config, sql, model_name)

        cls.commit_if_has_connection(config, model_name)

        return res

    @classmethod
    def drop_schema(cls, config, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)
        sql = cls.get_drop_schema_sql(config, schema)
        return cls.add_query(config, sql, model_name)

    @classmethod
    def already_exists(cls, config, schema, table, model_name=None):
        relation = cls.get_relation(config, schema=schema, identifier=table)
        return relation is not None

    @classmethod
    def quote(cls, identifier):
        return '"{}"'.format(identifier)

    @classmethod
    def _quote_as_configured(cls, config, identifier, quote_key):
        """This is the actual implementation of quote_as_configured, without
        the extra arguments needed for use inside materialization code.
        """
        assert quote_key in config.quoting
        if config.quoting[quote_key]:
            return cls.quote(identifier)
        else:
            return identifier

    @classmethod
    def quote_as_configured(cls, config, identifier, quote_key,
                            model_name=None):
        """Quote or do not quote the given identifer as configured in the
        project config for the quote key.

        The quote key should be one of 'database' (on bigquery, 'profile'),
        'identifier', or 'schema', or it will be treated as if you set `True`.
        """
        return cls._quote_as_configured(config, identifier, quote_key)

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_text_type` is not implemented for this adapter!')

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_number_type` is not implemented for this adapter!')

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_boolean_type` is not implemented for this adapter!')

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_datetime_type` is not implemented for this adapter!')

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_date_type` is not implemented for this adapter!')

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_time_type` is not implemented for this adapter!')

    @classmethod
    def convert_type(cls, agate_table, col_idx):
        return cls.convert_agate_type(agate_table, col_idx)

    @classmethod
    def convert_agate_type(cls, agate_table, col_idx):
        agate_type = agate_table.column_types[col_idx]
        conversions = [
            (agate.Text, cls.convert_text_type),
            (agate.Number, cls.convert_number_type),
            (agate.Boolean, cls.convert_boolean_type),
            (agate.DateTime, cls.convert_datetime_type),
            (agate.Date, cls.convert_date_type),
            (agate.TimeDelta, cls.convert_time_type),
        ]
        for agate_cls, func in conversions:
            if isinstance(agate_type, agate_cls):
                return func(agate_table, col_idx)

    ###
    # Operations involving the manifest
    ###
    @classmethod
    def run_operation(cls, config, manifest, operation_name):
        """Look the operation identified by operation_name up in the manifest
        and run it.

        Return an an AttrDict with three attributes: 'table', 'data', and
            'status'. 'table' is an agate.Table.
        """
        operation = manifest.find_operation_by_name(operation_name, 'dbt')

        # This causes a reference cycle, as dbt.context.runtime.generate()
        # ends up calling get_adapter, so the import has to be here.
        import dbt.context.runtime
        context = dbt.context.runtime.generate(
            operation,
            config,
            manifest,
        )

        result = operation.generator(context)()
        return result

    ###
    # Abstract methods involving the manifest
    ###
    @classmethod
    def get_catalog(cls, config, manifest):
        try:
            table = cls.run_operation(config, manifest,
                                      GET_CATALOG_OPERATION_NAME)
        finally:
            cls.release_connection(config, GET_CATALOG_OPERATION_NAME)

        schemas = list({
            node.schema.lower()
            for node in manifest.nodes.values()
        })

        results = table.where(lambda r: r['table_schema'].lower() in schemas)
        return results
