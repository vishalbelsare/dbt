from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

from dbt.task.test import TestTask


class TestSchemaTestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "test/integration/007_graph_selection_tests/models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {'git': 'https://github.com/fishtown-analytics/dbt-integration-project'}
            ]
        }

    def run_schema_and_assert(self, include, exclude, expected_tests):
        self.use_profile('postgres')
        self.use_default_project()

        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")
        self.run_dbt(["deps"])
        results = self.run_dbt()
        self.assertEqual(len(results), 5)

        args = FakeArgs()
        args.models = include
        args.exclude = exclude

        test_task = TestTask(args, self.config)
        test_results = test_task.run()

        ran_tests = sorted([test.node.get('name') for test in test_results])
        expected_sorted = sorted(expected_tests)

        self.assertEqual(ran_tests, expected_sorted)

    @attr(type='postgres')
    def test__postgres__schema_tests_no_specifiers(self):
        self.run_schema_and_assert(
            None,
            None,
            ['unique_emails_email',
             'unique_table_model_id',
             'unique_users_id',
             'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model(self):
        self.run_schema_and_assert(
            ['users'],
            None,
            ['unique_users_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_and_children(self):
        self.run_schema_and_assert(
            ['users+'],
            None,
            ['unique_users_id', 'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_and_parents(self):
        self.run_schema_and_assert(
            ['+users_rollup'],
            None,
            ['unique_users_id', 'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_and_parents_with_exclude(self):
        self.run_schema_and_assert(
            ['+users_rollup'],
            ['users_rollup'],
            ['unique_users_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_exclude_only(self):
        self.run_schema_and_assert(
            None,
            ['users_rollup'],
            ['unique_emails_email', 'unique_table_model_id', 'unique_users_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_in_pkg(self):
        self.run_schema_and_assert(
            ['test.users_rollup'],
            None,
            # TODO: change this. there's no way to select only direct ancestors
            # atm.
            ['unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_with_glob(self):
        self.run_schema_and_assert(
            ['*'],
            ['users'],
            ['unique_emails_email', 'unique_table_model_id', 'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_dep_package_only(self):
        self.run_schema_and_assert(
            ['dbt_integration_project'],
            None,
            ['unique_table_model_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_model_in_dep_pkg(self):
        self.run_schema_and_assert(
            ['dbt_integration_project.table_model'],
            None,
            ['unique_table_model_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_exclude_pkg(self):
        self.run_schema_and_assert(
            None,
            ['dbt_integration_project'],
            ['unique_emails_email', 'unique_users_id', 'unique_users_rollup_gender']
        )
