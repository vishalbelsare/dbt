from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest
import mock
import hashlib

from mock import call, ANY

import dbt.exceptions
import dbt.version
import dbt.tracking
import dbt.utils


class TestEventTracking(DBTIntegrationTest):
    maxDiff = None

    @property
    def profile_config(self):
        return {
            'config': {
                'send_anonymous_usage_stats': True
            }
        }

    @property
    def schema(self):
        return "event_tracking_033"

    @staticmethod
    def dir(path):
        return "test/integration/033_event_tracking_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    # TODO : Handle the subject. Should be the same every time!
    # TODO : Regex match a uuid for user_id, invocation_id?

    @mock.patch('dbt.tracking.tracker.track_struct_event')
    def run_event_test(
        self,
        cmd,
        expected_calls,
        expected_contexts,
        track_fn,
        expect_pass=True,
        expect_raise=False
    ):
        track_fn.reset_mock()

        project_id = hashlib.md5(
            self.config.project_name.encode('utf-8')).hexdigest()
        version = str(dbt.version.get_installed_version())

        if expect_raise:
            with self.assertRaises(BaseException):
                self.run_dbt(cmd, expect_pass=expect_pass)
        else:
            self.run_dbt(cmd, expect_pass=expect_pass)

        user_id = dbt.tracking.active_user.id
        invocation_id = dbt.tracking.active_user.invocation_id

        self.assertTrue(len(user_id) > 0)
        self.assertTrue(len(invocation_id) > 0)

        track_fn.assert_has_calls(expected_calls)

        ordered_contexts = []

        for (args, kwargs) in track_fn.call_args_list:
            ordered_contexts.append(
                [context.__dict__ for context in kwargs['context']]
            )

        populated_contexts = []

        for context in expected_contexts:
            if callable(context):
                populated_contexts.append(context(
                    project_id, user_id, invocation_id, version))
            else:
                populated_contexts.append(context)

        self.assertEqual(
            ordered_contexts,
            populated_contexts
        )

    def build_context(
        self,
        command,
        progress,
        result_type=None
    ):

        def populate(
            project_id,
            user_id,
            invocation_id,
            version
        ):
            return [
                {
                    'schema': 'iglu:com.dbt/invocation/jsonschema/1-0-0',
                    'data': {
                        'project_id': project_id,
                        'user_id': user_id,
                        'invocation_id': invocation_id,
                        'version': version,

                        'command': command,
                        'progress': progress,
                        'run_type': 'regular',

                        'options': None,  # TODO : Add options to compile cmd!
                        'result_type': result_type,
                        'result': None
                    }
                },
                {
                    'schema': 'iglu:com.dbt/platform/jsonschema/1-0-0',
                    'data': ANY
                },
                {
                    'schema': 'iglu:com.dbt/invocation_env/jsonschema/1-0-0',
                    'data': ANY
                }
            ]

        return populate

    def run_context(
        self,
        materialization,
        hashed_contents,
        model_id,
        index,
        total,
        status,
        error=None
    ):
        def populate(project_id, user_id, invocation_id, version):
            return [{
                'schema': 'iglu:com.dbt/run_model/jsonschema/1-0-0',
                'data': {
                    'invocation_id': invocation_id,

                    'model_materialization': materialization,

                    'execution_time': ANY,
                    'hashed_contents': hashed_contents,
                    'model_id': model_id,

                    'index': index,
                    'total': total,

                    'run_status': status,
                    'run_error': error,
                    'run_skipped': False,
                },
            }]

        return populate


class TestEventTrackingSuccess(TestEventTracking):
    @property
    def project_config(self):
        return {
            "data-paths": [self.dir("data")],
            "test-paths": [self.dir("test")],
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    @attr(type="postgres")
    def test__event_tracking_compile(self):
        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('compile', 'start'),
            self.build_context('compile', 'end', result_type='ok')
        ]

        self.run_event_test(
            ["compile", "--vars", "sensitive_thing: abc"],
            expected_calls,
            expected_contexts
        )

    @attr(type="postgres")
    def test__event_tracking_deps(self):
        package_context = [
            {
                'schema': 'iglu:com.dbt/package_install/jsonschema/1-0-0',
                'data': {
                    'name': 'c5552991412d1cd86e5c20a87f3518d5',
                    'source': 'git',
                    'version': 'eb0a191797624dd3a48fa681d3061212'
                }
            }
        ]

        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='package',
                label=ANY,
                property_='install',
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('deps', 'start'),
            package_context,
            self.build_context('deps', 'end', result_type='ok')
        ]

        self.run_event_test(["deps"], expected_calls, expected_contexts)

    @attr(type="postgres")
    def test__event_tracking_seed(self):
        def seed_context(project_id, user_id, invocation_id, version):
            return [{
                'schema': 'iglu:com.dbt/run_model/jsonschema/1-0-0',
                'data': {
                    'invocation_id': invocation_id,

                    'model_materialization': 'seed',

                    'execution_time': ANY,
                    'hashed_contents': '4f67ae18b42bc9468cc95ca0dab30531',
                    'model_id': '39bc2cd707d99bd3e600d2faaafad7ae',

                    'index': 1,
                    'total': 1,

                    'run_status': 'INSERT 1',
                    'run_error': None,
                    'run_skipped': False,
                },
            }]

        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='run_model',
                label=ANY,
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('seed', 'start'),
            seed_context,
            self.build_context('seed', 'end', result_type='ok')
        ]

        self.run_event_test(["seed"], expected_calls, expected_contexts)

    @attr(type="postgres")
    def test__event_tracking_models(self):
        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='run_model',
                label=ANY,
                context=ANY
            ),
            call(
                category='dbt',
                action='run_model',
                label=ANY,
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('run', 'start'),
            self.run_context(
                hashed_contents='1e5789d34cddfbd5da47d7713aa9191c',
                model_id='4fbacae0e1b69924b22964b457148fb8',
                index=1,
                total=2,
                status='CREATE VIEW',
                materialization='view'
            ),
            self.run_context(
                hashed_contents='20ff78afb16c8b3b8f83861b1d3b99bd',
                model_id='57994a805249953b31b738b1af7a1eeb',
                index=2,
                total=2,
                status='CREATE VIEW',
                materialization='view'
            ),
            self.build_context('run', 'end', result_type='ok')
        ]

        self.run_event_test(
            ["run", "--model", "example", "example_2"],
            expected_calls,
            expected_contexts
        )

    @attr(type="postgres")
    def test__event_tracking_model_error(self):
        # cmd = ["run", "--model", "model_error"]
        # self.run_event_test(cmd, event_run_model_error, expect_pass=False)

        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='run_model',
                label=ANY,
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('run', 'start'),
            self.run_context(
                hashed_contents='4419e809ce0995d99026299e54266037',
                model_id='576c3d4489593f00fad42b97c278641e',
                index=1,
                total=1,
                status='ERROR',
                materialization='view'
            ),
            self.build_context('run', 'end', result_type='ok')
        ]

        self.run_event_test(
            ["run", "--model", "model_error"],
            expected_calls,
            expected_contexts,
            expect_pass=False
        )

    @attr(type="postgres")
    def test__event_tracking_tests(self):
        # TODO: dbt does not track events for tests, but it should!
        self.run_dbt(["run", "--model", "example", "example_2"])

        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('test', 'start'),
            self.build_context('test', 'end', result_type='ok')
        ]

        self.run_event_test(
            ["test"],
            expected_calls,
            expected_contexts,
            expect_pass=False
        )


class TestEventTrackingCompilationError(TestEventTracking):
    @property
    def project_config(self):
        return {
            "source-paths": [self.dir("model-compilation-error")],
        }

    @attr(type="postgres")
    def test__event_tracking_with_compilation_error(self):
        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('compile', 'start'),
            self.build_context('compile', 'end', result_type='error')
        ]

        self.run_event_test(
            ["compile"],
            expected_calls,
            expected_contexts,
            expect_pass=False,
            expect_raise=True
        )


class TestEventTrackingUnableToConnect(TestEventTracking):

    @property
    def profile_config(self):
        return {
            'config': {
                'send_anonymous_usage_stats': True
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                    'noaccess': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': 'database',
                        'port': 5432,
                        'user': 'BAD',
                        'pass': 'bad_password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    }
                },
                'target': 'default2'
            }
        }

    @attr(type="postgres")
    def test__event_tracking_unable_to_connect(self):
        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('run', 'start'),
            self.build_context('run', 'end', result_type='error')
        ]

        self.run_event_test(
            ["run", "--target", "noaccess", "--models", "example"],
            expected_calls,
            expected_contexts,
            expect_pass=False
        )


class TestEventTrackingArchive(TestEventTracking):
    @property
    def project_config(self):
        return {
            "archive": [
                {
                    "source_schema": self.unique_schema(),
                    "target_schema": self.unique_schema(),
                    "tables": [
                        {
                            "source_table": "archivable",
                            "target_table": "archived",
                            "updated_at": '"updated_at"',
                            "unique_key": '"id"'
                        }
                    ]
                }
            ]
        }

    @attr(type="postgres")
    def test__event_tracking_archive(self):
        self.run_dbt(["run", "--models", "archivable"])

        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='run_model',
                label=ANY,
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('archive', 'start'),
            self.run_context(
                hashed_contents='f785c4490e73e5b52fed5627f5709bfa',
                model_id='3cdcd0fef985948fd33af308468da3b9',
                index=1,
                total=1,
                status='INSERT 0 1',
                materialization='archive'
            ),
            self.build_context('archive', 'end', result_type='ok')
        ]

        self.run_event_test(
            ["archive"],
            expected_calls,
            expected_contexts
        )


class TestEventTrackingCatalogGenerate(TestEventTracking):
    @attr(type="postgres")
    def test__event_tracking_catalog_generate(self):
        # create a model for the catalog
        self.run_dbt(["run", "--models", "example"])

        expected_calls = [
            call(
                category='dbt',
                action='invocation',
                label='start',
                context=ANY
            ),
            call(
                category='dbt',
                action='invocation',
                label='end',
                context=ANY
            ),
        ]

        expected_contexts = [
            self.build_context('generate', 'start'),
            self.build_context('generate', 'end', result_type='ok')
        ]

        self.run_event_test(
            ["docs", "generate"],
            expected_calls,
            expected_contexts
        )
