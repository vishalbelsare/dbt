import os
import re
import hashlib
import json

import dbt.exceptions
import dbt.flags
import dbt.utils

import dbt.clients.yaml_helper
import dbt.context.parser
import dbt.contracts.project

from dbt.node_types import NodeType
from dbt.compat import basestring, to_string
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import get_pseudo_test_path
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.parser.base import BaseParser

from .schemas_v1 import SchemaParserV1
from .schemas_v2 import SchemaParserV2


# TODO : make this say something about v1, link to docs
SYNTAX_ERROR_MESSAGE = """dbt was unable to parse the Schema file located at \
{path}.\n  This error occurred because the configuration you provided did not \
exactly\n  match the format that dbt expected. Tests in this file will not be \
run by dbt.\n  The encountered validation error is shown below:\n
  {error}"""


class SchemaParser(BaseParser):

    DEFAULT_SCHEMA_VERSION = 1

    @classmethod
    def get_test_path(cls, package_name, resource_name):
        return cls.get_path(NodeType.Test, package_name, resource_name)

    @classmethod
    def get_parser(cls, schema_version):
        parsers = {
            1: SchemaParserV1,
            2: SchemaParserV2
        }

        parser = parsers.get(schema_version)

        if not parser:
            raise RuntimeError("bad version") # TODO
        else:
            return parser

    @classmethod
    def parse_schema_test(cls, source_node, model_name, test_config, test_namespace,
                          test_name, root_project, source_package,
                          all_projects, macros=None):

        if isinstance(test_config, (basestring, int, float, bool)):
            test_args = {'arg': test_config}
        else:
            test_args = test_config

        # sort the dict so the keys are rendered deterministically (for tests)
        kwargs = [cls.as_kwarg(key, test_args[key]) for key in sorted(test_args)]

        # TODO : Clean this up!
        if test_namespace is None:
            macro_name = "test_{}".format(test_name)
            description_name = test_name
        else:
            macro_name = "{}.test_{}".format(test_namespace, test_name)
            description_name = "{}.{}".format(test_namespace, test_name)

        raw_sql = "{{{{ {macro}(model=ref('{model}'), {kwargs}) }}}}".format(**{
            'model': model_name,
            'macro': macro_name,
            'kwargs': ", ".join(kwargs)
        })

        base_path = source_node.get('path')
        hashed_name, full_name = cls.get_nice_schema_test_name(test_name, model_name,
                                                           test_args)

        hashed_path = get_pseudo_test_path(hashed_name, base_path, 'schema_test')
        full_path = get_pseudo_test_path(full_name, base_path, 'schema_test')

        # supply our own fqn which overrides the hashed version from the path
        fqn_override = cls.get_fqn(full_path, source_package)

        description = json.dumps({
            "test": description_name,
            "arguments": test_config
        })

        to_return = UnparsedNode(
            name=full_name,
            resource_type=NodeType.Test,
            package_name=source_node.get('package_name'),
            root_path=source_node.get('root_path'),
            path=hashed_path,
            original_file_path=source_node.get('original_file_path'),
            raw_sql=raw_sql,
            description=description
        )

        return cls.parse_node(to_return,
                          cls.get_test_path(source_node.get('package_name'),
                                        full_name),
                          root_project,
                          source_package,
                          all_projects,
                          tags=['schema'],
                          fqn_extra=None,
                          fqn=fqn_override,
                          macros=macros)

    @classmethod
    def get_nice_schema_test_name(cls, test_name, model_name, args):

        flat_args = []
        for arg_name in sorted(args):
            arg_val = args[arg_name]

            if isinstance(arg_val, dict):
                parts = arg_val.values()
            elif isinstance(arg_val, (list, tuple)):
                parts = arg_val
            else:
                parts = [arg_val]

            flat_args.extend([str(part) for part in parts])

        clean_flat_args = [re.sub('[^0-9a-zA-Z_]+', '_', arg) for arg in flat_args]
        unique = "__".join(clean_flat_args)

        cutoff = 32
        if len(unique) <= cutoff:
            label = unique
        else:
            label = hashlib.md5(unique.encode('utf-8')).hexdigest()

        filename = '{}_{}_{}'.format(test_name, model_name, label)
        name = '{}_{}_{}'.format(test_name, model_name, unique)

        return filename, name


    @classmethod
    def as_kwarg(cls, key, value):
        test_value = to_string(value)
        is_function = re.match(r'^\s*(ref|var)\s*\(.+\)\s*$', test_value)

        # if the value is a function, don't wrap it in quotes!
        if is_function:
            formatted_value = value
        else:
            formatted_value = value.__repr__()

        return "{key}={value}".format(key=key, value=formatted_value)

    @classmethod
    def get_test_context(cls, source_node, model_name, test_name, projects):
        package_name = source_node.get('package_name')
        test_namespace = None
        original_test_name = test_name
        split = test_name.split('.')

        if len(split) > 1:
            test_name = split[1]
            package_name = split[0]
            test_namespace = package_name

        source_package = projects.get(package_name)
        if source_package is None:
            desc = '"{}" test on model "{}"'.format(original_test_name, model_name)
            dbt.exceptions.raise_dep_not_found(test_node, desc, test_namespace)

        return {
            "test_name": test_name,
            "test_namespace": test_namespace,
            "source_package": source_package,
        }

    @classmethod
    def get_schema_tests(cls, model_name, schema_spec, root_project, all_projects, macros):
        to_return = {}

        source_node = schema_spec['source']
        for column in schema_spec['columns']:
            column_name = column['name']

            for test in column['tests']:
                test_context = {
                    "source_node": source_node,
                    "model_name": model_name,
                    "test_config": test['test_config'],
                    "root_project": root_project,
                    "all_projects": all_projects,
                    "macros": macros
                }

                test_context.update(cls.get_test_context(source_node,
                    model_name, test['test_name'], all_projects))

                test = cls.parse_schema_test(**test_context)
                to_return[test['unique_id']] = test

        return to_return

    @classmethod
    def get_sources(cls, source_name, schema_spec, root_project, all_projects, macros):
        source_node = schema_spec['source']

        package_name = source_node.get('package_name')
        path = cls.get_path(NodeType.Source, package_name, source_name)

        to_return = UnparsedNode(
            name=source_name,
            resource_type=NodeType.Source,
            package_name=package_name,
            root_path=source_node.get('root_path'),
            path=path,
            original_file_path=source_node.get('original_file_path'),
            raw_sql=schema_spec['sql_table_name'],
            description=schema_spec.get('description')
        )

        source_package = all_projects.get(source_node.get('package_name'))
        res = cls.parse_node(to_return,
                          path,
                          root_project,
                          source_package,
                          all_projects,
                          tags=['source'],
                          fqn_extra=None,
                          fqn=None, # ???
                          macros=macros)

        return {res['unique_id']: res}

    @classmethod
    def get_test_nodes(cls, schema_specs, root_project, all_projects, macros):
        to_return = {}

        for node_name, schema_spec in schema_specs.items():
            if schema_spec['resource_type'] == NodeType.Test:
                to_return.update(cls.get_schema_tests(node_name, schema_spec, root_project, all_projects, macros))
            elif schema_spec['resource_type'] == NodeType.Source:
                to_return.update(cls.get_sources(node_name, schema_spec, root_project, all_projects, macros))
                # sheit, we also need to return test nodes that are defined in sources
                # all of this is so bad lol
                # go outside, come back, clean up github issues. This is ok

        return to_return

    @classmethod
    def try_parse_yml_contents(cls, schema_file):
        raw_yml = schema_file.get('raw_yml')
        schema_name = "{}:{}".format(schema_file.get('package_name'), schema_file.get('path'))

        try:
            return dbt.clients.yaml_helper.load_yaml_text(raw_yml)
        except dbt.exceptions.ValidationException as e:
            logger.info("* Error parsing YAML in {}. dbt will skip this file."
                        "\n{}".format(schema_name, e))
            return None

    @classmethod
    def on_error(cls, schema_node, error):
        path = schema_node.get('original_file_path')
        dbt.utils.compiler_warning(path, SYNTAX_ERROR_MESSAGE.format(
                                   path=path, error=error.msg))

    @classmethod
    def normalize(cls, parser, schema_node, schema_spec):
        try:
            parsed = parser.parse(schema_node, schema_spec)
        except dbt.exceptions.ValidationException as e:
            # There was an error parsing the yml file. Kind of unfortunate, but
            # this throws away the whole file even if the error is confined
            # to a single test config. I think that this is acceptable, as the
            # schema.yml file is either valid or it's not. Trying to correct
            # user errors is tricky and error prone, so we're just not going to
            # do it. TODO : Confirm that this is acceptable behavior
            cls.on_error(schema_node, e)
            return None

        return parser.to_schema_spec(parsed)

    @classmethod
    def normalize_schemas(cls, schema_files, root_project, projects, macros=None):
        to_return = {}

        for schema_file in schema_files:
            schema_spec = cls.try_parse_yml_contents(schema_file)

            if schema_spec is None:
                continue

            # Pick the right parser for this version of a schema spec
            schema_version = schema_spec.get('version', cls.DEFAULT_SCHEMA_VERSION)
            parser = cls.get_parser(schema_version)

            parsed_schema_spec = cls.normalize(parser, schema_file, schema_spec)

            # Skip invalid schema specs
            if not parsed_schema_spec:
                continue

            # Check if models are defined in multiple places (this is an error)
            new_models = set(parsed_schema_spec.keys())
            seen_models = set(to_return.keys())
            if len(seen_models.intersection(new_models)) > 0:
                raise RuntimeError("dupe schema def!!")

            to_return = dbt.utils.merge(to_return, parsed_schema_spec)

        return to_return

    @classmethod
    def load_and_parse(cls, package_name, root_project, all_projects, root_dir,
                       relative_dirs, macros=None):
        extension = "[!.#~]*.yml"

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        file_matches = dbt.clients.system.find_matching(
            root_dir,
            relative_dirs,
            extension)

        result = []

        for file_match in file_matches:
            file_contents = dbt.clients.system.load_file_contents(
                file_match.get('absolute_path'), strip=False)

            original_file_path = os.path.join(file_match.get('searched_path'),
                                              file_match.get('relative_path'))

            parts = dbt.utils.split_path(file_match.get('relative_path', ''))
            name, _ = os.path.splitext(parts[-1])

            result.append({
                'name': name,
                'root_path': root_dir,
                'path': file_match.get('relative_path'),
                'original_file_path': original_file_path,
                'package_name': package_name,
                'raw_yml': file_contents
            })

        return cls.normalize_schemas(result, root_project, all_projects, macros)
