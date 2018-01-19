from __future__ import absolute_import

from contextlib import contextmanager

import dbt.compat
import dbt.exceptions
import dbt.flags as flags
import dbt.clients.gcloud

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger

import google.auth
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery

import time
import uuid


class BigQueryAdapter(PostgresAdapter):

    context_functions = [
        "query_for_existing",
        "execute_model",
        "drop",
        "execute",
        "quote_schema_and_table",
        "make_date_partitioned_table"
    ]

    SCOPE = ('https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive')

    QUERY_TIMEOUT = 300

    @classmethod
    def handle_error(cls, error, message, sql):
        logger.debug(message.format(sql=sql))
        logger.debug(error)
        error_msg = "\n".join([error['message'] for error in error.errors])
        raise dbt.exceptions.DatabaseException(error_msg)

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name='master'):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            message = "Bad request while running:\n{sql}"
            cls.handle_error(e, message, sql)

        except google.cloud.exceptions.Forbidden as e:
            message = "Access denied while running:\n{sql}"
            cls.handle_error(e, message, sql)

        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            raise dbt.exceptions.RuntimeException(dbt.compat.to_string(e))

    @classmethod
    def type(cls):
        return 'bigquery'

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def begin(cls, profile, name='master'):
        pass

    @classmethod
    def commit(cls, profile, connection):
        pass

    @classmethod
    def get_status(cls, cursor):
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!')

    @classmethod
    def get_bigquery_credentials(cls, config):
        method = config.get('method')
        creds = google.oauth2.service_account.Credentials

        if method == 'oauth':
            credentials, project_id = google.auth.default(scopes=cls.SCOPE)
            return credentials

        elif method == 'service-account':
            keyfile = config.get('keyfile')
            return creds.from_service_account_file(keyfile, scopes=cls.SCOPE)

        elif method == 'service-account-json':
            details = config.get('keyfile_json')
            return creds.from_service_account_info(details, scopes=cls.SCOPE)

        error = ('Invalid `method` in profile: "{}"'.format(method))
        raise dbt.exceptions.FailedToConnectException(error)

    @classmethod
    def get_bigquery_client(cls, config):
        project_name = config.get('project')
        creds = cls.get_bigquery_credentials(config)

        return google.cloud.bigquery.Client(project_name, creds)

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()
        credentials = connection.get('credentials', {})

        try:
            handle = cls.get_bigquery_client(credentials)

        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.info("Please log into GCP to continue")
            dbt.clients.gcloud.setup_default_credentials()

            handle = cls.get_bigquery_client(credentials)

        except Exception as e:
            raise
            logger.debug("Got an error when attempting to create a bigquery "
                         "client: '{}'".format(e))

            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        result['handle'] = handle
        result['state'] = 'open'
        return result

    @classmethod
    def query_for_existing(cls, profile, schemas, model_name=None):
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        all_tables = []
        for schema in schemas:
            dataset = cls.get_dataset(profile, schema, model_name)
            all_tables.extend(client.list_tables(dataset))

        relation_type_lookup = {
            'TABLE': 'table',
            'VIEW': 'view',
            'EXTERNAL': 'external'
        }

        existing = [(table.table_id, relation_type_lookup.get(table.table_type))
                    for table in all_tables]

        return dict(existing)

    @classmethod
    def drop(cls, profile, schema, relation, relation_type, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, schema, model_name)
        relation_object = dataset.table(relation)
        client.delete_table(relation_object)

    @classmethod
    def rename(cls, profile, schema, from_name, to_name, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!')

    @classmethod
    def get_timeout(cls, conn):
        credentials = conn['credentials']
        return credentials.get('timeout_seconds', cls.QUERY_TIMEOUT)

    @classmethod
    def materialize_as_view(cls, profile, dataset, model):
        model_name = model.get('name')
        model_sql = model.get('injected_sql')

        view = dataset.table(model_name)
        view.view_query = model_sql
        view.view_use_legacy_sql = False

        logger.debug("Model SQL ({}):\n{}".format(model_name, model_sql))

        with cls.exception_handler(profile, model_sql, model_name, model_name):
            view.create()

        if view.created is None:
            msg = "Error creating view {}".format(model_name)
            raise dbt.exceptions.RuntimeException(msg)

        return "CREATE VIEW"

    @classmethod
    def poll_until_job_completes(cls, job, timeout):
        retry_count = timeout

        while retry_count > 0 and job.state != 'DONE':
            retry_count -= 1
            time.sleep(1)
            job.reload()

        if job.state != 'DONE':
            raise dbt.exceptions.RuntimeException("BigQuery Timeout Exceeded")

        elif job.error_result:
            raise job.exception()

    @classmethod
    def make_date_partitioned_table(cls, profile, dataset_name, identifier, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, dataset_name, identifier)
        table_ref = dataset.table(identifier)
        table = google.cloud.bigquery.Table(table_ref)
        table.partitioning_type = 'DAY'
        return client.create_table(table)

    @classmethod
    def materialize_as_table(cls, profile, dataset, model, decorator=None):
        model_name = model.get('name')
        model_sql = model.get('injected_sql')
        partition_type = model.get('config', '{}').get('partition_type')

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        if decorator is None:
            table_name = model_name
        else:
            table_name = "{}${}".format(model_name, decorator)

        table_ref = dataset.table(table_name)
        job_config = google.cloud.bigquery.QueryJobConfig()
        job_config.destination = table_ref
        job_config.write_disposition = 'WRITE_TRUNCATE'

        logger.debug("Model SQL ({}):\n{}".format(table_name, model_sql))
        query_job = client.query(model_sql, job_config=job_config)

        with cls.exception_handler(profile, model_sql, model_name, model_name):
            # this waits for the job to complete
            iterator = query_job.result(timeout=cls.get_timeout(conn))

        # TODO : use this elsewhere!
        cls.release_connection(profile, model_name)

        return "CREATE TABLE"

    @classmethod
    def execute_model(cls, profile, model, materialization, decorator=None, model_name=None):

        if flags.STRICT_MODE:
            connection = cls.get_connection(profile, model.get('name'))
            validate_connection(connection)
            cls.release_connection(profile, model.get('name'))

        model_name = model.get('name')
        model_schema = model.get('schema')

        dataset = cls.get_dataset(profile, model_schema, model_name)

        if materialization == 'view':
            res = cls.materialize_as_view(profile, dataset, model)
        elif materialization == 'table':
            res = cls.materialize_as_table(profile, dataset, model, decorator)
        else:
            msg = "Invalid relation type: '{}'".format(materialization)
            raise dbt.exceptions.RuntimeException(msg, model)

        return res

    @classmethod
    def fetch_query_results(cls, query):
        all_rows = []

        rows = query.rows
        token = query.page_token

        while True:
            all_rows.extend(rows)
            if token is None:
                break
            rows, total_count, token = query.fetch_data(page_token=token)
        return all_rows

    @classmethod
    def execute(cls, profile, sql, model_name=None, fetch=False, **kwargs):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        query = client.run_sync_query(sql)
        query.timeout_ms = cls.get_timeout(conn) * 1000
        query.use_legacy_sql = False

        debug_message = "Fetching data for query {}:\n{}"
        logger.debug(debug_message.format(model_name, sql))

        query.run()

        res = []
        if fetch:
            res = cls.fetch_query_results(query)

        status = 'ERROR' if query.errors else 'OK'
        return status, res

    @classmethod
    def execute_and_fetch(cls, profile, sql, model_name, auto_begin=None):
        return cls.execute(profile, sql, model_name, fetch=True)

    @classmethod
    def add_begin_query(cls, profile, name):
        raise dbt.exceptions.NotImplementedException(
            '`add_begin_query` is not implemented for this adapter!')

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)

        dataset = cls.get_dataset(profile, schema, model_name)

        # TODO: should this use client.create_dataset(dataset)?
        with cls.exception_handler(profile, 'create dataset', model_name):
            dataset.create()

    @classmethod
    def drop_tables_in_schema(cls, dataset):
        for table in dataset.list_tables():
            table.delete()

    @classmethod
    def drop_schema(cls, profile, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)

        if not cls.check_schema_exists(profile, schema, model_name):
            return

        dataset = cls.get_dataset(profile, schema, model_name)

        with cls.exception_handler(profile, 'drop dataset', model_name):
            cls.drop_tables_in_schema(dataset)
            dataset.delete()

    @classmethod
    def get_existing_schemas(cls, profile, model_name=None):
        conn = cls.get_connection(profile, model_name)

        client = conn.get('handle')

        with cls.exception_handler(profile, 'list dataset', model_name):
            all_datasets = client.list_datasets()
            return [ds.dataset_id for ds in all_datasets]

    @classmethod
    def get_columns_in_table(cls, profile, schema_name, table_name,
                             model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`get_columns_in_table` is not implemented for this adapter!')

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        conn = cls.get_connection(profile, model_name)

        client = conn.get('handle')

        with cls.exception_handler(profile, 'get dataset', model_name):
            all_datasets = client.list_datasets()
            return any([ds.name == schema for ds in all_datasets])

    @classmethod
    def get_dataset(cls, profile, dataset_name, model_name=None):
        conn = cls.get_connection(profile, model_name)

        client = conn.get('handle')
        dataset = client.dataset(dataset_name)
        return dataset

    @classmethod
    def warning_on_hooks(cls, hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        dbt.ui.printer.print_timestamped_line(msg.format(hook_type),
                                              dbt.ui.printer.COLOR_FG_YELLOW)

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True):
        if model_name in ['on-run-start', 'on-run-end']:
            cls.warning_on_hooks(model_name)
        else:
            raise dbt.exceptions.NotImplementedException(
                '`add_query` is not implemented for this adapter!')

    @classmethod
    def is_cancelable(cls):
        return False

    @classmethod
    def quote(cls, identifier):
        return '`{}`'.format(identifier)

    @classmethod
    def quote_schema_and_table(cls, profile, schema, table, model_name=None):
        connection = cls.get_connection(profile)
        credentials = connection.get('credentials', {})
        project = credentials.get('project')
        return '{}.{}.{}'.format(cls.quote(project),
                                 cls.quote(schema),
                                 cls.quote(table))
