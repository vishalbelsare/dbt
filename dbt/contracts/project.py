from dbt.api.object import APIObject
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.utils import deep_merge
from dbt.contracts.connection import POSTGRES_CREDENTIALS_CONTRACT, \
    REDSHIFT_CREDENTIALS_CONTRACT, SNOWFLAKE_CREDENTIALS_CONTRACT, \
    BIGQUERY_CREDENTIALS_CONTRACT

# TODO: add description fields.
ARCHIVE_TABLE_CONFIG_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'source_table': {'type': 'string'},
        'target_table': {'type': 'string'},
        'updated_at': {'type': 'string'},
        'unique_key': {'type': 'string'},
    },
    'required': ['source_table', 'target_table', 'updated_at', 'unique_key'],
}


ARCHIVE_CONFIG_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'source_schema': {'type': 'string'},
        'target_schema': {'type': 'string'},
        'tables': {
            'type': 'array',
            'item': ARCHIVE_TABLE_CONFIG_CONTRACT,
        }
    },
    'required': ['source_schema', 'target_schema', 'tables'],
}


PROJECT_CONTRACT = {
    'type': 'object',
    'description': 'The project configuration.',
    'additionalProperties': False,
    'properties': {
        'name': {
            'type': 'string',
        },
        'version': {
            'type': 'string',
            # I got this from here, it seems reasonable enough:
            # https://github.com/sindresorhus/semver-regex/blob/944928/index.js
            'pattern': (
                r'^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[\da-z-]+'
                r'(?:\.[\da-z-]+)*)?(?:\+[\da-z-]+(?:\.[\da-z-]+)*)?$'
            ),
        },
        'source-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'macro-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'data-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'test-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'analysis-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'docs-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'target-path': {
            'type': 'string',
        },
        'clean-targets': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'profile': {
            'type': 'string',
        },
        'log-path': {
            'type': 'string',
        },
        'models': {
            'type': 'object',
            'additionalProperties': True,
        },
        'on-run-start': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'on-run-end': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'archive': ARCHIVE_CONFIG_CONTRACT,
    },
    'required': ['name', 'version'],
}


class Project(APIObject):
    SCHEMA = PROJECT_CONTRACT


CONFIG_CONTRACT = deep_merge(
    PROJECT_CONTRACT,
    {
        'properties': {
            'send_anonymous_usage_stats': {
                'type': 'boolean',
            },
            'use_colors': {
                'type': 'boolean',
            },
            'threads': {
                'type': 'number',
            },
            'credentials': {
                'anyOf': [
                    POSTGRES_CREDENTIALS_CONTRACT,
                    REDSHIFT_CREDENTIALS_CONTRACT,
                    SNOWFLAKE_CREDENTIALS_CONTRACT,
                    BIGQUERY_CREDENTIALS_CONTRACT,
                ],
            }
        }
    }
)


class Configuration(APIObject):
    SCHEMA = CONFIG_CONTRACT


PROJECTS_LIST_PROJECT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': PROJECT_CONTRACT,
    },
}


class ProjectList(APIObject):
    SCHEMA = PROJECTS_LIST_PROJECT
