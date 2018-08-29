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


class PostgresCredentials(Credentials):
    SCHEMA = POSTGRES_CREDENTIALS_CONTRACT


class RedshiftCredentials(Credentials):
    SCHEMA = REDSHIFT_CREDENTIALS_CONTRACT


class SnowflakeCredentials(Credentials):
    SCHEMA = SNOWFLAKE_CREDENTIALS_CONTRACT


class BigQueryCredentials(Credentials):
    SCHEMA = BIGQUERY_CREDENTIALS_CONTRACT


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
    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        # this will validate itself in its own __init__.
        self._credentials = create_credentials(self.type,
                                               self._contents['credentials'])

    @property
    def credentials(self):
        return self._credentials
