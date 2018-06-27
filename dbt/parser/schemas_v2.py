
import os
import re
import hashlib

import dbt.exceptions
import dbt.flags
import dbt.utils

import dbt.clients.yaml_helper
import dbt.context.parser
import dbt.contracts.project

from dbt.contracts.graph.schema_spec import RawSchemaSpecV2, ParsedSchemaSpec

from dbt.node_types import NodeType
from dbt.compat import basestring, to_string
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import get_pseudo_test_path
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.parser.base import BaseParser


# TODO : make this say something about v2, link to docs
SYNTAX_ERROR_MESSAGE = """dbt was unable to parse the Schema file located at \
{path}.\n  This error occurred because the configuration you provided did not \
exactly\n  match the format that dbt expected. Tests in this file will not be \
run by dbt.\n  The encountered validation error is shown below:\n
  {error}"""

class SchemaParserV2(BaseParser):

    @classmethod
    def normalize_tests(cls, column_tests):
        normalized_tests = []
        for column_test in column_tests:

            if isinstance(column_test, str):
                test_name = column_test
                config = {}
            elif isinstance(column_test, dict):
                test_name = list(column_test.keys()).pop()
                config = column_test[test_name]
            else:
                raise RuntimeError("bad")

            normalized_tests.append({
                "test_name": test_name,
                "test_config": config
            })

        return normalized_tests

    @classmethod
    def to_schema_spec(cls, parsed):
        schema_spec = parsed.data
        model_schemas = schema_spec['models']

        to_return = {}

        for model_schema in model_schemas:
            model_name = model_schema['name']
            to_return[model_name] = {
                "name": model_name,
                "description": model_schema.get('description'),
                "options": model_schema.get('options', {}),
                "columns": [],
                "source": parsed.source,
            }

            seen_columns = set()

            for column in model_schema['columns']:
                column_name = column['name']

                if column_name in seen_columns:
                    raise RuntimeError("Duplicate column specified: {}".format(column_name))
                seen_columns.add(column_name)

                column_def = {
                    "name" : column_name,
                    "description": column.get('description'),
                    "tests": cls.normalize_tests(column.get('tests', []))
                }

                to_return[model_name]['columns'].append(column_def)

        return ParsedSchemaSpec(**to_return)

    @classmethod
    def parse(cls, schema_node, schema_data):
        data = {
            "source": schema_node,
            "data": schema_data,
        }
        return RawSchemaSpecV2(**data)
