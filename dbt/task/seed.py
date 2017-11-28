import os
from dbt.node_runners import SeedRunner
from dbt.node_types import NodeType
from dbt.runner import RunManager
from dbt.seeder import Seeder
from dbt.task.base_task import RunnableTask
import dbt.ui.printer


class SeedTask(RunnableTask):
    def run(self):
        runner = RunManager(
            self.project,
            self.project["target-path"],
            self.args,
        )
        query = {
            "include": ["*"],
            "exclude": [],
            "resource_types": [NodeType.Seed],
        }
        results = runner.run_flat(query, SeedRunner)
        dbt.ui.printer.print_run_end_messages(results)
        return results
