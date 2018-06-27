
import os
import re
import hashlib
import collections
import itertools

import dbt.exceptions
import dbt.flags
import dbt.utils

import dbt.clients.yaml_helper
import dbt.context.parser
import dbt.contracts.project

from dbt.contracts.graph.schema_spec import RawSchemaSpecV1, ParsedSchemaSpec

from dbt.node_types import NodeType
from dbt.compat import basestring, to_string
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import get_pseudo_test_path
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.parser.base import BaseParser


class SchemaParserV1(BaseParser):
    AmbiguousFieldGuess = '*'  # I suppose this could also be None

    @classmethod
    def get_column_for_test(cls, test_name, config):

        if test_name in ['unique', 'not_null']:
            return config

        elif test_name in ['accepted_values', 'relationships']:
            return config.get('field', cls.AmbiguousFieldGuess)

        else:
            return cls.AmbiguousFieldGuess  # Is this a really bad idea?

    @classmethod
    def parse_column_fragments(cls, model_name, test_name, test_configs):
        column_fragments = []
        for test_config in test_configs:
            column_name = cls.get_column_for_test(test_name, test_config)
            column_fragments.append({
                "model_name": model_name,
                "test_name": test_name,
                "column_name": column_name,
                "test_config": test_config
            })

        return column_fragments

    @classmethod
    def parse_constraints(cls, model_name, constraints):
        column_fragments = []

        for test_name, test_configs in constraints.items():
            fragments = cls.parse_column_fragments(model_name, test_name,
                                                   test_configs)
            column_fragments.extend(fragments)

        return column_fragments

    @classmethod
    def parse_model_constraints(cls, model_schema_yml):
        all_column_fragments = []
        for model_name, model_config in model_schema_yml.items():
            constraints = model_config['constraints']
            column_fragments = cls.parse_constraints(model_name, constraints)
            all_column_fragments.extend(column_fragments)

        return all_column_fragments

    @classmethod
    def group_by(cls, records, key):
        get_key = lambda record: record[key]

        # The list needs to be sorted (as with uniq in bash)
        sorted_records = sorted(records, key=get_key)

        return {
            key: list(group) for (key, group)
            in itertools.groupby(sorted_records, get_key)
        }

    @classmethod
    def prune_test_fields(cls, column_tests):
        return [{
            "test_name": test['test_name'],
            "test_config": test['test_config']
        } for test in column_tests]

    @classmethod
    def to_IR(cls, records, source):
        by_model = cls.group_by(records, 'model_name')

        to_return = {}
        for model_name, column_tests in by_model.items():
            by_column = cls.group_by(column_tests, 'column_name')

            by_column_list = [
                {
                    "name": column_name,
                    "description": None,
                    "tests": cls.prune_test_fields(column_tests)
                }
                for column_name, column_tests in by_column.items()
            ]

            to_return[model_name] = {
                "name": model_name,
                "description": None,
                "options": {},
                "columns": by_column_list,
                "source": source
            }

        return to_return

    @classmethod
    def to_schema_spec(cls, parsed):
        data = parsed.data
        col_fragments = cls.parse_model_constraints(data)
        schema_spec = cls.to_IR(col_fragments, parsed.source)

        return ParsedSchemaSpec(**schema_spec)

    @classmethod
    def parse(cls, schema_source, schema_data):
        data = {
            "source": schema_source,
            "data": schema_data,
        }
        return RawSchemaSpecV1(**data)
