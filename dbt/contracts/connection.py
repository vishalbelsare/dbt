from dbt.api.object import APIObject
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

POSTGRES_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'dbname': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'pass': {
            'type': 'string',
        },
        'port': {
            'oneOf': [
                {
                    'type': 'integer',
                    'minimum': 0,
                    'maximum': 65535,
                },
                {
                    'type': 'string'
                },
            ],
        },
        'schema': {
            'type': 'string',
        },
        'keepalives_idle': {
            'type': 'integer',
        },
    },
    'required': ['dbname', 'host', 'user', 'pass', 'port', 'schema'],
}

REDSHIFT_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'method': {
            'enum': ['database', 'iam'],
            'description': (
                'database: use user/pass creds; iam: use temporary creds'
            ),
        },
        'dbname': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'pass': {
            'type': 'string',
        },
        'port': {
            'oneOf': [
                {
                    'type': 'integer',
                    'minimum': 0,
                    'maximum': 65535,
                },
                {
                    'type': 'string'
                },
            ],
        },
        'schema': {
            'type': 'string',
        },
        'cluster_id': {
            'type': 'string',
            'description': (
                'If using IAM auth, the name of the cluster'
            )
        },
        'iam_duration_seconds': {
            'type': 'integer',
            'minimum': 900,
            'maximum': 3600,
            'description': (
                'If using IAM auth, the ttl for the temporary credentials'
            )
        },
        'keepalives_idle': {
            'type': 'integer',
        },
        'required': ['dbname', 'host', 'user', 'port', 'schema']
    }
}

SNOWFLAKE_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'account': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'warehouse': {
            'type': 'string',
        },
        'role': {
            'type': 'string',
        },
    },
    'required': ['account', 'user', 'password', 'database', 'schema'],
}

BIGQUERY_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'method': {
            'enum': ['oauth', 'service-account', 'service-account-json'],
        },
        'project': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'keyfile': {
            'type': 'string',
        },
        'keyfile_json': {
            'type': 'object',
        },
        'timeout_seconds': {
            'type': 'integer',
        },
    },
    'required': ['method', 'project', 'schema'],
}


CONNECTION_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'type': {
            'enum': ['postgres', 'redshift', 'snowflake', 'bigquery'],
        },
        'name': {
            'type': ['null', 'string'],
        },
        'state': {
            'enum': ['init', 'open', 'closed', 'fail'],
        },
        'transaction_open': {
            'type': 'boolean',
        },
        'handle': {
            'type': ['null', 'object'],
        },
        'credentials': {
            'description': (
                'The credentials object here should match the connection type.'
            ),
            'anyOf': [
                POSTGRES_CREDENTIALS_CONTRACT,
                REDSHIFT_CREDENTIALS_CONTRACT,
                SNOWFLAKE_CREDENTIALS_CONTRACT,
                BIGQUERY_CREDENTIALS_CONTRACT,
            ],
        }
    },
    'required': [
        'type', 'name', 'state', 'transaction_open', 'handle', 'credentials'
    ],
}


class Credentials(APIObject):
    """Common base class for credentials. This is not valid to instantiate"""
    SCHEMA = NotImplemented
    @property
    def type(self):
        raise NotImplementedError(
            'type not implemented for base credentials class'
        )

    def copy(self):
        return self.replace()

    def replace(self, *args, **kwargs):
        serialized = self.serialize()
        serialized.update(*args, **kwargs)
        return self.__class__(**serialized)


class PostgresCredentials(Credentials):
    SCHEMA = POSTGRES_CREDENTIALS_CONTRACT
    @property
    def type(self):
        return 'postgres'

    @property
    def password(self):
        # we can't access this as 'pass' since that's reserved
        return self._contents['pass']


class RedshiftCredentials(Credentials):
    SCHEMA = REDSHIFT_CREDENTIALS_CONTRACT
    @property
    def type(self):
        return 'redshift'


class SnowflakeCredentials(Credentials):
    SCHEMA = SNOWFLAKE_CREDENTIALS_CONTRACT
    @property
    def type(self):
        return 'snowflake'


class BigQueryCredentials(Credentials):
    SCHEMA = BIGQUERY_CREDENTIALS_CONTRACT
    @property
    def type(self):
        return 'bigquery'


CREDENTIALS_MAPPING = {
    'postgres': PostgresCredentials,
    'redshift': RedshiftCredentials,
    'snowflake': SnowflakeCredentials,
    'bigquery': BigQueryCredentials,
}


def create_credentials(typename, credentials):
    if typename not in CREDENTIALS_MAPPING:
        dbt.exceptions.raise_unrecognized_credentials_type(
            typename, CREDENTIALS_MAPPING.keys()
        )
    cls = CREDENTIALS_MAPPING[typename]
    return cls(**credentials)


class Connection(APIObject):
    SCHEMA = CONNECTION_CONTRACT
    def __init__(self, credentials, *args, **kwargs):
        # this is a bit clunky but we deserialize and then reserialize for now
        if isinstance(credentials, Credentials):
            credentials = credentials.serialize()
        super(Connection, self).__init__(credentials=credentials,
                                         *args, **kwargs)
        # this will validate itself in its own __init__.
        self._credentials = create_credentials(self.type,
                                               self._contents['credentials'])

    @property
    def credentials(self):
        return self._credentials

    name = named_property('name', 'The name of this connection')
    handle = named_property('handle', 'The handle to the database connection')
    state = named_property('state', 'The state of the connection')
    transaction_open = named_property(
        'transaction_open',
        'True if there is an open transaction, False otherwise.'
    )

