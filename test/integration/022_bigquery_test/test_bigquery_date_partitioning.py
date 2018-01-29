from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

from dbt.task.test import TestTask
from dbt.project import read_project


class TestBigqueryDatePartitioning(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "test/integration/022_bigquery_test/dp-models"

    def run_schema_validations(self):
        project = read_project('dbt_project.yml')
        args = FakeArgs()

        test_task = TestTask(args, project)
        return test_task.run()

    @attr(type='bigquery')
    def test__bigquery_date_partitioning(self):
        self.use_profile('bigquery')
        self.use_default_project()
        self.run_dbt()

        test_results = self.run_schema_validations()

        self.assertTrue(len(test_results) > 0)
        for result in test_results:
            self.assertFalse(result.errored)
            self.assertFalse(result.skipped)
            # status = # of failing rows
            self.assertEqual(result.status, 0)

