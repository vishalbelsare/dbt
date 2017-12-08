import multiprocessing

from dbt.adapters.postgres import PostgresAdapter
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


drop_lock = multiprocessing.Lock()


class RedshiftAdapter(PostgresAdapter):

    @classmethod
    def type(cls):
        return 'redshift'

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def drop(cls, profile, schema, relation, relation_type, model_name=None):
        global drop_lock

        to_return = None

        try:
            drop_lock.acquire()

            connection = cls.get_connection(profile, model_name)

            if connection.get('transaction_open'):
                cls.commit(profile, connection)

            cls.begin(profile, connection.get('name'))

            to_return = super(PostgresAdapter, cls).drop(
                profile, schema, relation, relation_type, model_name)

            cls.commit(profile, connection)
            cls.begin(profile, connection.get('name'))

            return to_return

        finally:
            drop_lock.release()

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode("utf-8")) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)
