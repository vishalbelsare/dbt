from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile


class TestSimpleBigQueryRun(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "test/integration/022_bigquery_test/models"

    @property
    def project_config(self):
        return {
            'data-paths': ['test/integration/022_bigquery_test/data'],
            'macro-paths': ['test/integration/022_bigquery_test/macros'],
        }

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @use_profile('bigquery')
    def test__bigquery_simple_run(self):
        # make sure seed works twice. Full-refresh is a no-op
        self.run_dbt(['seed'])
        self.run_dbt(['seed', '--full-refresh'])
        results = self.run_dbt()
        self.assertEqual(len(results), 2)

        # The 'dupe' model should fail, but all others should pass
        test_results = self.run_dbt(['test'], expect_pass=False)

        for result in test_results:
            if 'dupe' in result.node.get('name'):
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                self.assertTrue(result.status > 0)

            # assert that actual tests pass
            else:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                # status = # of failing rows
                self.assertEqual(result.status, 0)

    @use_profile('bigquery')
    def test__bigquery_exists_non_destructive(self):
        self.run_dbt(['seed'])
        # first run dbt. this should work
        self.assertEqual(len(self.run_dbt()), 2)
        # then run dbt with --non-destructive. The view should still exist.
        self.run_dbt(['run', '--non-destructive'])
        # The 'dupe' model should fail, but all others should pass
        test_results = self.run_dbt(['test'], expect_pass=False)

        for result in test_results:
            if 'dupe' in result.node.get('name'):
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                self.assertTrue(result.status > 0)

            # assert that actual tests pass
            else:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                # status = # of failing rows
                self.assertEqual(result.status, 0)
