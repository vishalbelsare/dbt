import os.path
from copy import deepcopy

import dbt.exceptions
import dbt.clients.yaml_helper
import dbt.clients.system
from dbt.contracts.connection import Connection, create_credentials
from dbt.contracts.project import Project, Configuration
from dbt.context.common import env_var
from dbt import compat

from dbt.logger import GLOBAL_LOGGER as logger


INVALID_PROFILE_MESSAGE = """
dbt encountered an error while trying to read your profiles.yml file.

{error_string}
"""


def read_profile(profiles_dir):
    path = os.path.join(profiles_dir, 'profiles.yml')

    contents = None
    if os.path.isfile(path):
        try:
            contents = dbt.clients.system.load_file_contents(path, strip=False)
            return dbt.clients.yaml_helper.load_yaml_text(contents)
        except dbt.exceptions.ValidationException as e:
            msg = INVALID_PROFILE_MESSAGE.format(error_string=e)
            raise dbt.exceptions.ValidationException(msg)

    return {}


def read_config(profiles_dir):
    profile = read_profile(profiles_dir)
    if profile is None:
        return {}
    else:
        return profile.get('config', {})


def send_anonymous_usage_stats(config):
    return config.get('send_anonymous_usage_stats', True)


def colorize_output(config):
    return config.get('use_colors', True)


DEFAULT_THREADS = 1
DEFAULT_SEND_ANONYMOUS_USAGE_STATS = True
DEFAULT_USE_COLORS = True
DEFAULT_PROFILES_DIR = os.path.join(os.path.expanduser('~'), '.dbt')


def _render(value, ctx):
    if isinstance(value, compat.basestring):
        return dbt.clients.jinja.get_rendered(value, ctx)
    else:
        return value


def _get_profile(args, profile_name=None):
    """Given the raw profiles as read from disk, the name of the desired
    profile, and the name of the non-default target to use, if specified,
    return the global user configuration, the selected profile, and its name.
    """
    if args.profile_name is not None:
        profile_name = args.profile_name
    if profile_name is None:
        raise RuntimeError(
            'Profile name must be set in the project file, or via --project '
            '(TODO: real error)')

    profiles_dir = getattr(args, 'profiles_dir', DEFAULT_PROFILES_DIR)
    raw_profiles = read_profile(profiles_dir)

    if profile_name not in raw_profiles:
        raise RuntimeError('{} not in raw_profiles (TODO: real error)'
                           .format(profile_name))

    profile = raw_profiles[profile_name]
    if 'outputs' not in profile:
        raise RuntimeError(
            'outputs not specified in profile {} (TODO: real error)'
            .format(profile_name))

    if target_name_override is not None:
        target = target_name_override
    elif 'target' in profile:
        target = profile['target']
    else:
        raise RuntimeError(
            'target not specified in profile {} (TODO: real error)'
            .format(profile_name))

    if target not in profile['outputs']:
        raise RuntimeError('target {} not in profile {} (TODO: real error)'
                           .format(target, profile_name))

    cfg = raw_profiles.get('config', {})

    # if entries are strings, we want to render them so we can get any
    # environment variables that might store important credentials elements.
    credentials = {
        k: _render(v, {'env_var': env_var})
        for k, v in profile['outputs'][target].items()
    }

    return cfg, credentials, profile_name


class RuntimeConfig(object):
    """The runtime configuration, as constructed from its components. There's a
    lot because there is a lot of stuff!
    TODO:
        - make credentials/threads optional for some commands (dbt deps should not care)
            - via subclassing/superclassing, probably
        - consider splitting project stuff out into its own sub-object?
            - I'd prefer not to
    """
    def __init__(self, project_name, version, source_paths, macro_paths,
                 data_paths, test_paths, analysis_paths, docs_paths,
                 target_path, clean_targets, log_path, models, on_run_start,
                 on_run_end, archive, profile_name, send_anonymous_usage_stats,
                 use_colors, threads, credentials):
        # 'project'
        self.project_name = project_name
        self.version = version
        self.source_paths = source_paths
        self.macro_paths = macro_paths
        self.data_paths = data_paths
        self.test_paths = test_paths
        self.analysis_paths = analysis_paths
        self.docs_paths = docs_paths
        self.target_path = target_path
        self.clean_targets = clean_targets
        self.log_path = log_path
        self.models = models
        self.on_run_start = on_run_start
        self.on_run_end = on_run_end
        self.archive = archive
        # 'profile'
        self.profile_name = profile_name
        self.send_anonymous_usage_stats = send_anonymous_usage_stats
        self.use_colors = use_colors
        self.threads = threads
        self.credentials = credentials

    @classmethod
    def from_project_config(cls, project_dict, args):
        """Create a RuntimeConfig from a dbt_project.yml file's configuration
        contents and the command-line arguments.
        """
        # just for validation.
        Project(**project_dict)

        # name/version are required in the Project definition, so we can assume
        # they are present
        name = project_dict['name']
        version = project_dict['version']
        source_paths = project_dict.get('source-paths', ['models'])
        macro_paths = project_dict.get('macro-paths', ['macros'])
        data_paths = project_dict.get('data-paths', ['data'])
        test_paths = project_dict.get('test-paths', ['test'])
        analysis_paths = project_dict.get('analysis-paths', [])
        docs_paths = project_dict.get('docs-paths', source_paths[:]),
        target_path = project_dict.get('target-path', 'target')
        clean_targets = project_dict.get('clean-targets', [target_path])
        profile = project_dict.get('profile')
        log_path = project_dict.get('log-path', 'logs')
        models = project_dict.get('models', {})
        on_run_start = project_dict.get('on-run-start', [])
        on_run_end = project_dict.get('on-run-end', [])
        archive = project_dict.get('archive', {})

        # now that we have most of the defaults we need, read in the profile
        # based on the given arguments. Note that profile might be None, but if
        # it's set via args then that's ok.
        user_cfg, selected_profile, profile_name = _get_profile(args, profile)

        # valid connections never include the number of threads, but it's
        # stored on a per-connection level in the raw configs
        threads = selected_profile.pop('threads', DEFAULT_THREADS)
        if hasattr(args, 'threads') and args.threads is not None:
            threads = args.threads

        # credentials carry their 'type' in their actual type, not their
        # attributes. We do want this in order to pick our Credentials class.
        if 'type' not in selected_profile:
            raise dbt.exceptions.ValidationException(
                'required field "type" not found in profile {}'
                .format(profile_name))
        typename = selected_profile.pop('type')
        credentials = create_credentials(typename, selected_profile)

        send_anonymous_usage_stats = user_cfg.get(
            'send_anonymous_usage_stats',
            DEFAULT_SEND_ANONYMOUS_USAGE_STATS
        )
        use_colors = user_cfg.get('use_colors', DEFAULT_USE_COLORS)

        return cls(
            project_name=name,
            version=version,
            source_paths=source_paths,
            macro_paths=macro_paths,
            data_paths=data_paths,
            test_paths=test_paths,
            analysis_paths=analysis_paths,
            docs_paths=docs_paths,
            target_path=target_path,
            clean_targets=clean_targets,
            log_path=log_path,
            models=models,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            archive=archive,
            profile_name=profile_name,
            send_anonymous_usage_stats=send_anonymous_usage_stats,
            use_colors=use_colors,
            threads=threads,
            credentials=credentials
        )

    def to_project_config(self):
        return deepcopy({
            'name': self.project_name,
            'version': self.version,
            'source-paths': self.source-paths,
            'macro-paths': self.macro_paths,
            'data-paths': self.data_paths,
            'test-paths': self.test_paths,
            'analysis_paths': self.analysis-paths,
            'docs-paths': self.docs_paths,
            'target-path': self.target_path,
            'clean-targets': self.clean_targets,
            'log-path': self.log_path,
            'models': self.models,
            'on-run-start': self.on_run_start,
            'on-run-end': self.on_run_end,
            'archive': self.archive,
            'profile': self.profile_name,
        })

    def serialize(self):
        result = self.to_project_config()
        result.update(deepcopy({
            'send_anonymous_usage_stats': self.send_anonymous_usage_stats,
            'use_colors': self.use_colors,
            'threads': self.threads,
            'credentials': self.credentials.serialize(),
        }))
        return result

    def validate(self):
        Config(**self.serialize())
