
import os
import re
import hashlib

import dbt.exceptions
import dbt.flags
import dbt.utils

import dbt.clients.yaml_helper
import dbt.context.parser
import dbt.contracts.project

from dbt.contracts.graph.schema_spec import RawSchemaSpecV2, ParsedSchemaSpec, ParsedSourceSpec

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
    def normalize_tests(cls, column_name, column_tests):
        normalized_tests = []
        for column_test in column_tests:

            if isinstance(column_test, basestring):
                test_name = column_test
                config = {"arg": column_name}
            elif isinstance(column_test, dict):
                test_name = list(column_test.keys()).pop()
                config = {"column_name": column_name}
                config.update(column_test[test_name])
            else:
                raise RuntimeError("bad")

            normalized_tests.append({
                "test_name": test_name,
                "test_config": config
            })

        return normalized_tests

    @classmethod
    def parse_schema_spec_columns(cls, schema):
        to_return = []
        seen_columns = set()

        for column in schema['columns']:
            column_name = column['name']

            if column_name in seen_columns:
                raise RuntimeError("Duplicate column specified: {}".format(column_name))
            seen_columns.add(column_name)

            column_def = {
                "name" : column_name,
                "description": column.get('description'),
                "tests": cls.normalize_tests(column_name, column.get('tests', []))
            }

            to_return.append(column_def)

        return to_return

    @classmethod
    def parse_schema_spec_models(cls, parsed, model_schemas):
        to_return = {}

        for model_schema in model_schemas:
            model_name = model_schema['name']
            to_return[model_name] = {
                "name": model_name,
                "resource_type": NodeType.Test,
                "description": model_schema.get('description'),
                "options": model_schema.get('options', {}),
                "columns": cls.parse_schema_spec_columns(model_schema),
                "source": parsed.source,
            }

        return ParsedSchemaSpec(**to_return)

    @classmethod
    def parse_schema_spec_sources(cls, parsed, source_schemas):
        to_return = {}

        for source_schema in source_schemas:
            source_name = source_schema.get('name')

            for table in source_schema.get('tables', []):
                table_name = table.get('name')

                to_return[table_name] = {
                    # These are duplicated for every source table. Is that dumb? TODO
                    "parent": {
                        "name": source_schema.get('name'),
                        "description": source_schema.get('description'),
                    },
                    "resource_type": NodeType.Source,
                    "name": table_name,
                    "sql_table_name": table.get("sql_table_name"),
                    "description": table.get('description'),
                    "options": table.get('options', {}),
                    "columns": cls.parse_schema_spec_columns(table),
                    "source": parsed.source,
                }

        return ParsedSourceSpec(**to_return)

    @classmethod
    def to_schema_spec(cls, parsed):
        schema_spec = parsed.data

        model_schemas = cls.parse_schema_spec_models(parsed, schema_spec['models']).serialize()
        source_schemas = cls.parse_schema_spec_sources(parsed, schema_spec['sources']).serialize()

        return dbt.utils.merge(source_schemas, model_schemas)


    @classmethod
    def parse(cls, schema_node, schema_data):
        data = {
            "source": schema_node,
            "data": schema_data,
        }
        return RawSchemaSpecV2(**data)
