import random
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_runners import SeedRunner
from dbt.node_types import NodeType
from dbt.runner import RunManager
from dbt.task.base_task import RunnableTask
import dbt.ui.printer


class SeedTask(RunnableTask):
    def run(self):
        runner = RunManager(self.config)
        query = {
            "include": ["*"],
            "exclude": [],
            "resource_types": [NodeType.Seed],
        }
        results = runner.run_flat(query, SeedRunner)

        if self.args.show:
            self.show_tables(results)

        dbt.ui.printer.print_run_end_messages(results)
        return results

    def show_table(self, result):
        table = result.node['agate_table']
        rand_table = table.order_by(lambda x: random.random())

        schema = result.node['schema']
        alias = result.node['alias']

        header = "Random sample of table: {}.{}".format(schema, alias)
        logger.info("")
        logger.info(header)
        logger.info("-" * len(header))
        rand_table.print_table(max_rows=10, max_columns=None)
        logger.info("")

    def show_tables(self, results):
        for result in results:
            if not result.errored:
                self.show_table(result)
