import unittest
import mock

import os
import yaml

import dbt.flags
import dbt.parser
from dbt.parser import ModelParser, MacroParser, DataTestParser, SchemaParser, ParserUtils
from dbt.utils import timestring
from dbt.config import RuntimeConfig

from dbt.node_types import NodeType
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedNode, ParsedMacro, ParsedNodePatch
from dbt.contracts.graph.unparsed import UnparsedNode

def get_os_path(unix_path):
    return os.path.normpath(unix_path)


class ParserTest(unittest.TestCase):

    def find_input_by_name(self, models, name):
        return next(
            (model for model in models if model.get('name') == name),
            {})

    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        profile_data = {
            'target': 'test',
            'quoting': {},
            'outputs': {
                'test': {
                    'type': 'postgres',
                    'host': 'localhost',
                    'schema': 'analytics',
                    'user': 'test',
                    'pass': 'test',
                    'dbname': 'test',
                    'port': 1,
                }
            }
        }

        root_project = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }


        self.root_project_config = RuntimeConfig.from_parts_or_dicts(
            project=root_project,
            profile=profile_data
        )

        snowplow_project = {
            'name': 'snowplow',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('./dbt_modules/snowplow'),
        }

        self.snowplow_project_config = RuntimeConfig.from_parts_or_dicts(
            project=snowplow_project, profile=profile_data
        )

        self.model_config = {
            'enabled': True,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
        }

        self.disabled_config = {
            'enabled': False,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
        }

    def test__single_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'raw_sql': ("select * from events"),
        }]

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    original_file_path='model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )

    def test__single_model__nested_configuration(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'original_file_path': 'nested/path/model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'path': get_os_path('nested/path/model_one.sql'),
            'raw_sql': ("select * from events"),
        }]

        self.root_project_config.models = {
            'materialized': 'ephemeral',
            'root': {
                'nested': {
                    'path': {
                        'materialized': 'ephemeral'
                    }
                }
            }
        }

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'nested', 'path', 'model_one'],
                    empty=False,
                    package_name='root',
                    original_file_path='nested/path/model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=ephemeral_config,
                    tags=[],
                    path=get_os_path('nested/path/model_one.sql'),
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )

    def test__empty_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'model_one.sql',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': (" "),
        }]

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config}),
            {
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=True,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': [],
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    original_file_path='model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )

    def test__simple_dependency(self):
        models = [{
            'name': 'base',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'base.sql',
            'original_file_path': 'base.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from events'
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'original_file_path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': "select * from {{ref('base')}}"
        }]

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.base': ParsedNode(
                    alias='base',
                    name='base',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.base',
                    fqn=['root', 'base'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='base.sql',
                    original_file_path='base.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'base').get('raw_sql'),
                    description='',
                    columns={}

                ),
                'model.root.events_tx': ParsedNode(
                    alias='events_tx',
                    name='events_tx',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.events_tx',
                    fqn=['root', 'events_tx'],
                    empty=False,
                    package_name='root',
                    refs=[['base']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events_tx.sql',
                    original_file_path='events_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events_tx').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )

    def test__multiple_dependencies(self):
        models = [{
            'name': 'events',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events.sql',
            'original_file_path': 'events.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions.sql',
            'original_file_path': 'sessions.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'original_file_path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions_tx.sql',
            'original_file_path': 'sessions_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'original_file_path': 'multi.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with s as (select * from {{ref('sessions_tx')}}), "
                        "e as (select * from {{ref('events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.events': ParsedNode(
                    alias='events',
                    name='events',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.events',
                    fqn=['root', 'events'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events.sql',
                    original_file_path='events.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.sessions': ParsedNode(
                    alias='sessions',
                    name='sessions',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.sessions',
                    fqn=['root', 'sessions'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions.sql',
                    original_file_path='sessions.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions').get('raw_sql'),
                    description='',
                    columns={},
                ),
                'model.root.events_tx': ParsedNode(
                    alias='events_tx',
                    name='events_tx',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.events_tx',
                    fqn=['root', 'events_tx'],
                    empty=False,
                    package_name='root',
                    refs=[['events']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events_tx.sql',
                    original_file_path='events_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.sessions_tx': ParsedNode(
                    alias='sessions_tx',
                    name='sessions_tx',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.sessions_tx',
                    fqn=['root', 'sessions_tx'],
                    empty=False,
                    package_name='root',
                    refs=[['sessions']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions_tx.sql',
                    original_file_path='sessions_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.multi': ParsedNode(
                    alias='multi',
                    name='multi',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.multi',
                    fqn=['root', 'multi'],
                    empty=False,
                    package_name='root',
                    refs=[['sessions_tx'], ['events_tx']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='multi.sql',
                    original_file_path='multi.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'multi').get('raw_sql'),
                    description='',
                    columns={}
                ),
            }
        )

    def test__multiple_dependencies__packages(self):
        models = [{
            'name': 'events',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'events.sql',
            'original_file_path': 'events.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions.sql',
            'original_file_path': 'sessions.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'events_tx.sql',
            'original_file_path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions_tx.sql',
            'original_file_path': 'sessions_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'original_file_path': 'multi.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with s as "
                        "(select * from {{ref('snowplow', 'sessions_tx')}}), "
                        "e as "
                        "(select * from {{ref('snowplow', 'events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.snowplow.events': ParsedNode(
                    alias='events',
                    name='events',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.events',
                    fqn=['snowplow', 'events'],
                    empty=False,
                    package_name='snowplow',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events.sql',
                    original_file_path='events.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.sessions': ParsedNode(
                    alias='sessions',
                    name='sessions',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.sessions',
                    fqn=['snowplow', 'sessions'],
                    empty=False,
                    package_name='snowplow',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions.sql',
                    original_file_path='sessions.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.events_tx': ParsedNode(
                    alias='events_tx',
                    name='events_tx',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.events_tx',
                    fqn=['snowplow', 'events_tx'],
                    empty=False,
                    package_name='snowplow',
                    refs=[['events']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events_tx.sql',
                    original_file_path='events_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.sessions_tx': ParsedNode(
                    alias='sessions_tx',
                    name='sessions_tx',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.sessions_tx',
                    fqn=['snowplow', 'sessions_tx'],
                    empty=False,
                    package_name='snowplow',
                    refs=[['sessions']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions_tx.sql',
                    original_file_path='sessions_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.multi': ParsedNode(
                    alias='multi',
                    name='multi',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.multi',
                    fqn=['root', 'multi'],
                    empty=False,
                    package_name='root',
                    refs=[['snowplow', 'sessions_tx'],
                             ['snowplow', 'events_tx']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='multi.sql',
                    original_file_path='multi.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'multi').get('raw_sql'),
                    description='',
                    columns={}
                ),
            }
        )

    def test__process_refs__packages(self):
        graph = {
            'macros': {},
            'nodes': {
                'model.snowplow.events': {
                    'name': 'events',
                    'alias': 'events',
                    'schema': 'analytics',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.events',
                    'fqn': ['snowplow', 'events'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.disabled_config,
                    'tags': [],
                    'path': 'events.sql',
                    'original_file_path': 'events.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'raw_sql': 'does not matter'
                },
                'model.root.events': {
                    'name': 'events',
                    'alias': 'events',
                    'schema': 'analytics',
                    'resource_type': 'model',
                    'unique_id': 'model.root.events',
                    'fqn': ['root', 'events'],
                    'empty': False,
                    'package_name': 'root',
                    'refs': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': [],
                    'path': 'events.sql',
                    'original_file_path': 'events.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'raw_sql': 'does not matter'
                },
                'model.root.dep': {
                    'name': 'dep',
                    'alias': 'dep',
                    'schema': 'analytics',
                    'resource_type': 'model',
                    'unique_id': 'model.root.dep',
                    'fqn': ['root', 'dep'],
                    'empty': False,
                    'package_name': 'root',
                    'refs': [['events']],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': [],
                    'path': 'multi.sql',
                    'original_file_path': 'multi.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'raw_sql': 'does not matter'
                }
            }
        }

        manifest = Manifest(
            nodes={k: ParsedNode(**v) for (k,v) in graph['nodes'].items()},
            macros={k: ParsedMacro(**v) for (k,v) in graph['macros'].items()},
            docs={},
            generated_at=timestring(),
        )

        processed_manifest = ParserUtils.process_refs(manifest, 'root')
        self.assertEqual(
            processed_manifest.to_flat_graph(),
            {
                'macros': {},
                'nodes': {
                    'model.snowplow.events': {
                        'name': 'events',
                        'alias': 'events',
                        'schema': 'analytics',
                        'resource_type': 'model',
                        'unique_id': 'model.snowplow.events',
                        'fqn': ['snowplow', 'events'],
                        'empty': False,
                        'package_name': 'snowplow',
                        'refs': [],
                        'depends_on': {
                            'nodes': [],
                            'macros': []
                        },
                        'config': self.disabled_config,
                        'tags': [],
                        'path': 'events.sql',
                        'original_file_path': 'events.sql',
                        'root_path': get_os_path('/usr/src/app'),
                        'raw_sql': 'does not matter',
                        'agate_table': None,
                        'columns': {},
                        'description': '',
                    },
                    'model.root.events': {
                        'name': 'events',
                        'alias': 'events',
                        'schema': 'analytics',
                        'resource_type': 'model',
                        'unique_id': 'model.root.events',
                        'fqn': ['root', 'events'],
                        'empty': False,
                        'package_name': 'root',
                        'refs': [],
                        'depends_on': {
                            'nodes': [],
                            'macros': []
                        },
                        'config': self.model_config,
                        'tags': [],
                        'path': 'events.sql',
                        'original_file_path': 'events.sql',
                        'root_path': get_os_path('/usr/src/app'),
                        'raw_sql': 'does not matter',
                        'agate_table': None,
                        'columns': {},
                        'description': '',
                    },
                    'model.root.dep': {
                        'name': 'dep',
                        'alias': 'dep',
                        'schema': 'analytics',
                        'resource_type': 'model',
                        'unique_id': 'model.root.dep',
                        'fqn': ['root', 'dep'],
                        'empty': False,
                        'package_name': 'root',
                        'refs': [['events']],
                        'depends_on': {
                            'nodes': ['model.root.events'],
                            'macros': []
                        },
                        'config': self.model_config,
                        'tags': [],
                        'path': 'multi.sql',
                        'original_file_path': 'multi.sql',
                        'root_path': get_os_path('/usr/src/app'),
                        'raw_sql': 'does not matter',
                        'agate_table': None,
                        'columns': {},
                        'description': '',
                    }
                }
            }
        )

    def test__in_model_config(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'model_one.sql',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': [],
                    },
                    config=self.model_config,
                    tags=[],
                    root_path=get_os_path('/usr/src/app'),
                    path='model_one.sql',
                    original_file_path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )

    def test__root_project_config(self):
        self.root_project_config.models = {
            'materialized': 'ephemeral',
            'root': {
                'view': {
                    'materialized': 'view'
                }
            }
        }

        models = [{
            'name': 'table',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'table.sql',
            'original_file_path': 'table.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'original_file_path': 'ephemeral.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'original_file_path': 'view.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        view_config = self.model_config.copy()
        view_config.update({
            'materialized': 'view'
        })

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.table': ParsedNode(
                    alias='table',
                    name='table',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.table',
                    fqn=['root', 'table'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='table.sql',
                    original_file_path='table.sql',
                    config=self.model_config,
                    tags=[],
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'table').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.ephemeral': ParsedNode(
                    alias='ephemeral',
                    name='ephemeral',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    config=ephemeral_config,
                    tags=[],
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.view': ParsedNode(
                    alias='view',
                    name='view',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='view.sql',
                    original_file_path='view.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=view_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql'),
                    description='',
                    columns={}
                ),
            }

        )

    def test__other_project_config(self):
        self.root_project_config.models = {
            'materialized': 'ephemeral',
            'root': {
                'view': {
                    'materialized': 'view'
                }
            },
            'snowplow': {
                'enabled': False,
                'views': {
                    'materialized': 'view',
                    'multi_sort': {
                        'enabled': True,
                        'materialized': 'table'
                    }
                }
            }
        }

        self.snowplow_project_config.models = {
            'snowplow': {
                'enabled': False,
                'views': {
                    'materialized': 'table',
                    'sort': 'timestamp',
                    'multi_sort': {
                        'sort': ['timestamp', 'id'],
                    }
                }
            }
        }

        models = [{
            'name': 'table',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'table.sql',
            'original_file_path': 'table.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'original_file_path': 'ephemeral.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'original_file_path': 'view.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'disabled',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'disabled.sql',
            'original_file_path': 'disabled.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'package',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': get_os_path('views/package.sql'),
            'original_file_path': get_os_path('views/package.sql'),
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'multi_sort',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': get_os_path('views/multi_sort.sql'),
            'original_file_path': get_os_path('views/multi_sort.sql'),
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        view_config = self.model_config.copy()
        view_config.update({
            'materialized': 'view'
        })

        disabled_config = self.model_config.copy()
        disabled_config.update({
            'enabled': False,
            'materialized': 'ephemeral'
        })

        sort_config = self.model_config.copy()
        sort_config.update({
            'enabled': False,
            'materialized': 'view',
            'sort': 'timestamp',
        })

        multi_sort_config = self.model_config.copy()
        multi_sort_config.update({
            'materialized': 'table',
            'sort': ['timestamp', 'id']
        })

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.table': ParsedNode(
                    alias='table',
                    name='table',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.table',
                    fqn=['root', 'table'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='table.sql',
                    original_file_path='table.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=self.model_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'table').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.ephemeral': ParsedNode(
                    alias='ephemeral',
                    name='ephemeral',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=ephemeral_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.view': ParsedNode(
                    alias='view',
                    name='view',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='view.sql',
                    original_file_path='view.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=view_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'view').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.multi_sort': ParsedNode(
                    alias='multi_sort',
                    name='multi_sort',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.multi_sort',
                    fqn=['snowplow', 'views', 'multi_sort'],
                    empty=False,
                    package_name='snowplow',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path=get_os_path('views/multi_sort.sql'),
                    original_file_path=get_os_path('views/multi_sort.sql'),
                    root_path=get_os_path('/usr/src/app'),
                    config=multi_sort_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'multi_sort').get('raw_sql'),
                    description='',
                    columns={}
                ),
            }
        )

    def test__simple_schema_v1_test(self):
        test_yml = yaml.safe_load(
            '{model_one: {constraints: {not_null: [id],'
            'unique: [id],'
            'accepted_values: [{field: id, values: ["a","b"]}],'
            'relationships: [{from: id, to: ref(\'model_two\'), field: id}]' # noqa
            '}}}'
        )
        results = list(SchemaParser.parse_v1_test_yml(
            original_file_path='test_one.yml',
            test_yml=test_yml,
            package_name='root',
            root_project=self.root_project_config,
            all_projects={
                'root': self.root_project_config,
                'snowplow': self.snowplow_project_config
            },
            root_dir=get_os_path('/usr/src/app')
        ))
        results.sort(key=lambda n: n.name)

        not_null_sql = "{{ test_not_null(model=ref('model_one'), arg='id') }}"
        unique_sql = "{{ test_unique(model=ref('model_one'), arg='id') }}"
        accepted_values_sql = "{{ test_accepted_values(model=ref('model_one'), field='id', values=['a', 'b']) }}" # noqa
        relationships_sql = "{{ test_relationships(model=ref('model_one'), field='id', from='id', to=ref('model_two')) }}" # noqa

        expected = [
            ParsedNode(
                alias='accepted_values_model_one_id__a__b',
                name='accepted_values_model_one_id__a__b',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.accepted_values_model_one_id__a__b', # noqa
                fqn=['root', 'schema_test',
                        'accepted_values_model_one_id__a__b'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                depends_on={
                    'nodes': [],
                    'macros': []
                },
                config=self.model_config,
                path=get_os_path(
                    'schema_test/accepted_values_model_one_id__a__b.sql'),
                tags=['schema'],
                raw_sql=accepted_values_sql,
                description='',
                columns={}
            ),
            ParsedNode(
                alias='not_null_model_one_id',
                name='not_null_model_one_id',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.not_null_model_one_id',
                fqn=['root', 'schema_test', 'not_null_model_one_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                depends_on={
                    'nodes': [],
                    'macros': []
                },
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path(
                    'schema_test/not_null_model_one_id.sql'),
                tags=['schema'],
                raw_sql=not_null_sql,
                description='',
                columns={}
            ),
            ParsedNode(
                alias='relationships_model_one_id__id__ref_model_two_',
                name='relationships_model_one_id__id__ref_model_two_',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.relationships_model_one_id__id__ref_model_two_', # noqa
                fqn=['root', 'schema_test',
                        'relationships_model_one_id__id__ref_model_two_'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one'], ['model_two']],
                depends_on={
                    'nodes': [],
                    'macros': []
                },
                config=self.model_config,
                path=get_os_path('schema_test/relationships_model_one_id__id__ref_model_two_.sql'), # noqa
                tags=['schema'],
                raw_sql=relationships_sql,
                description='',
                columns={}
            ),
            ParsedNode(
                alias='unique_model_one_id',
                name='unique_model_one_id',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.unique_model_one_id',
                fqn=['root', 'schema_test', 'unique_model_one_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                depends_on={
                    'nodes': [],
                    'macros': []
                },
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path('schema_test/unique_model_one_id.sql'),
                tags=['schema'],
                raw_sql=unique_sql,
                description='',
                columns={}
            ),
        ]
        self.assertEqual(
            results,
            expected,
        )

    def test__simple_schema_v2(self):
        test_yml = yaml.safe_load(
            '{models: [{name: model_one, description: "blah blah", columns: ['
            '{name: id, description: "user ID", tests: [unique, not_null, '
            '{accepted_values: {values: ["a", "b"]}},'
            '{relationships: {from: id, to: ref(\'model_two\')}}]'
            '}], tests: [some_test: { key: value }]}]}'
        )
        results = list(SchemaParser.parse_v2_yml(
            original_file_path='test_one.yml',
            test_yml=test_yml,
            package_name='root',
            root_project=self.root_project_config,
            all_projects={
                'root': self.root_project_config,
                'snowplow': self.snowplow_project_config
            },
            root_dir=get_os_path('/usr/src/app'),
            macros=None
        ))

        # split this into tests and patches, assert there's nothing else
        tests = sorted((node for t, node in results if t == 'test'),
                       key=lambda n: n.name)
        patches = sorted((node for t, node in results if t == 'patch'),
                         key=lambda n: n.name)
        self.assertEqual(len(tests)+len(patches), len(results))

        not_null_sql = "{{ test_not_null(model=ref('model_one'), column_name='id') }}"
        unique_sql = "{{ test_unique(model=ref('model_one'), column_name='id') }}"
        accepted_values_sql = "{{ test_accepted_values(model=ref('model_one'), column_name='id', values=['a', 'b']) }}" # noqa
        relationships_sql = "{{ test_relationships(model=ref('model_one'), column_name='id', from='id', to=ref('model_two')) }}" # noqa
        some_test_sql = "{{ test_some_test(model=ref('model_one'), key='value') }}"

        expected_tests = [
            ParsedNode(
                alias='accepted_values_model_one_id__a__b',
                name='accepted_values_model_one_id__a__b',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.accepted_values_model_one_id__a__b',
                fqn=['root', 'schema_test',
                        'accepted_values_model_one_id__a__b'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path(
                    'schema_test/accepted_values_model_one_id__a__b.sql'),
                tags=['schema'],
                raw_sql=accepted_values_sql,
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='not_null_model_one_id',
                name='not_null_model_one_id',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.not_null_model_one_id',
                fqn=['root', 'schema_test', 'not_null_model_one_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path('schema_test/not_null_model_one_id.sql'),
                tags=['schema'],
                raw_sql=not_null_sql,
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='relationships_model_one_id__id__ref_model_two_',
                name='relationships_model_one_id__id__ref_model_two_',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.relationships_model_one_id__id__ref_model_two_', # noqa
                fqn=['root', 'schema_test',
                        'relationships_model_one_id__id__ref_model_two_'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one'], ['model_two']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path('schema_test/relationships_model_one_id__id__ref_model_two_.sql'), # noqa
                tags=['schema'],
                raw_sql=relationships_sql,
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='some_test_model_one_value',
                name='some_test_model_one_value',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.some_test_model_one_value',
                fqn=['root', 'schema_test', 'some_test_model_one_value'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path('schema_test/some_test_model_one_value.sql'),
                tags=['schema'],
                raw_sql=some_test_sql,
                description='',
                columns={}
            ),
            ParsedNode(
                alias='unique_model_one_id',
                name='unique_model_one_id',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.unique_model_one_id',
                fqn=['root', 'schema_test', 'unique_model_one_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path('schema_test/unique_model_one_id.sql'),
                tags=['schema'],
                raw_sql=unique_sql,
                description='',
                columns={},
                column_name='id'
            ),
        ]
        for test, expected in zip(tests, expected_tests):
            self.assertEqual(test, expected)

        expected_patches = [
            ParsedNodePatch(name='model_one',
                description='blah blah',
                original_file_path='test_one.yml',
                columns={
                    'id': {
                    'name': 'id',
                    'description': 'user ID',
                }},
                docrefs=[],
            ),
        ]
        for patch, expected in zip(patches, expected_patches):
            self.assertEqual(patch, expected)

    @mock.patch.object(SchemaParser, 'find_schema_yml')
    @mock.patch.object(dbt.parser.schemas, 'logger')
    def test__schema_v2_as_v1(self, mock_logger, find_schema_yml):
        test_yml = yaml.safe_load(
            '{models: [{name: model_one, description: "blah blah", columns: ['
            '{name: id, description: "user ID", tests: [unique, not_null, '
            '{accepted_values: {values: ["a", "b"]}},'
            '{relationships: {from: id, to: ref(\'model_two\')}}]'
            '}], tests: [some_test: { key: value }]}]}'
        )
        find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
        root_project = {}
        all_projects = {}
        root_dir = '/some/path'
        relative_dirs = ['a', 'b']
        with self.assertRaises(dbt.exceptions.CompilationException) as cm:
            dbt.parser.schemas.SchemaParser.load_and_parse(
                'test', root_project, all_projects, root_dir, relative_dirs
            )
            self.assertIn('https://docs.getdbt.com/v0.11/docs/schemayml-files',
                          str(cm.exception))

    @mock.patch.object(SchemaParser, 'build_node')
    @mock.patch.object(SchemaParser, 'find_schema_yml')
    @mock.patch.object(dbt.parser.schemas, 'logger')
    def test__schema_v1_version_model(self, mock_logger, find_schema_yml, build_node):
        test_yml = yaml.safe_load(
            '{model_one: {constraints: {not_null: [id],'
            'unique: [id],'
            'accepted_values: [{field: id, values: ["a","b"]}],'
            'relationships: [{from: id, to: ref(\'model_two\'), field: id}]' # noqa
            '}}, version: {constraints: {not_null: [id]}}}'
        )
        find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
        root_project = {}
        all_projects = {}
        root_dir = '/some/path'
        relative_dirs = ['a', 'b']
        dbt.parser.schemas.SchemaParser.load_and_parse(
            'test', root_project, all_projects, root_dir, relative_dirs
        )

    def test__simple_data_test(self):
        tests = [{
            'name': 'no_events',
            'resource_type': 'test',
            'package_name': 'root',
            'path': 'no_events.sql',
            'original_file_path': 'no_events.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': "select * from {{ref('base')}}"
        }]

        self.assertEqual(
            DataTestParser.parse_sql_nodes(
                tests,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'test.root.no_events': ParsedNode(
                    alias='no_events',
                    name='no_events',
                    schema='analytics',
                    resource_type='test',
                    unique_id='test.root.no_events',
                    fqn=['root', 'no_events'],
                    empty=False,
                    package_name='root',
                    refs=[['base']],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    path='no_events.sql',
                    original_file_path='no_events.sql',
                    root_path=get_os_path('/usr/src/app'),
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        tests, 'no_events').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )

    def test__simple_macro(self):
        macro_file_contents = """
{% macro simple(a, b) %}
  {{a}} + {{b}}
{% endmacro %}
"""

        result = MacroParser.parse_macro_file(
            macro_file_path='simple_macro.sql',
            macro_file_contents=macro_file_contents,
            root_path=get_os_path('/usr/src/app'),
            package_name='root',
            resource_type=NodeType.Macro)

        self.assertTrue(callable(result['macro.root.simple'].generator))

        self.assertEqual(
            result,
            {
                'macro.root.simple': {
                    'name': 'simple',
                    'resource_type': 'macro',
                    'unique_id': 'macro.root.simple',
                    'package_name': 'root',
                    'depends_on': {
                        'macros': []
                    },
                    'original_file_path': 'simple_macro.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'tags': [],
                    'path': 'simple_macro.sql',
                    'raw_sql': macro_file_contents,
                }
            }
        )

    def test__simple_macro_used_in_model(self):
        macro_file_contents = """
{% macro simple(a, b) %}
  {{a}} + {{b}}
{% endmacro %}
"""

        result = MacroParser.parse_macro_file(
            macro_file_path='simple_macro.sql',
            macro_file_contents=macro_file_contents,
            root_path=get_os_path('/usr/src/app'),
            package_name='root',
            resource_type=NodeType.Macro)

        self.assertTrue(callable(result['macro.root.simple'].generator))

        self.assertEqual(
            result,
            {
                'macro.root.simple': {
                    'name': 'simple',
                    'resource_type': 'macro',
                    'unique_id': 'macro.root.simple',
                    'package_name': 'root',
                    'depends_on': {
                        'macros': []
                    },
                    'original_file_path': 'simple_macro.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'tags': [],
                    'path': 'simple_macro.sql',
                    'raw_sql': macro_file_contents,
                }
            }
        )

        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'raw_sql': ("select *, {{package.simple(1, 2)}} from events"),
        }]

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    original_file_path='model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )

    def test__macro_no_explicit_project_used_in_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'original_file_path': 'model_one.sql',
            'raw_sql': ("select *, {{ simple(1, 2) }} from events"),
        }]

        self.assertEqual(
            ModelParser.parse_sql_nodes(
                models,
                self.root_project_config,
                {'root': self.root_project_config,
                 'snowplow': self.snowplow_project_config}),
            {
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    original_file_path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }
        )
