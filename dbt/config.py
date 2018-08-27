import os.path
from copy import deepcopy

import dbt.exceptions
import dbt.clients.yaml_helper
import dbt.clients.system
from dbt.contracts.connection import Connection, create_credentials

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

    return cfg, profile['outputs'][target], profile_name



class RuntimeConfig(object):
    """The runtime configuration, as constructed from its components. There's a
    lot because there is a lot of stuff!
    TODO:
        - make credentials/threads optional for some commands (dbt deps should not care)
            - via subclassing/superclassing, probably
        - make sure this is possible/easy to serialize/deseralize to dicts
            - should be pretty easy, everything is a native obj or an APIObject
        - consider splitting project stuff out into its own sub-object
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
    def from_args_defaults(self, name, version, args, source_paths=None,
                           macro_paths=None, data_paths=None, test_paths=None,
                           analysis_paths=None, docs_paths=None,
                           target_path='target', clean_targets=None,
                           log_path='logs', profile=None, models=None,
                           on_run_start=None, on_run_end=None, archive=None):
        # first, handle defaults if they exist. Name and version are mandatory
        # and have no default values. profile is only mandatory if not
        # specified in the command-line args.
        if source_paths is None:
            source_paths = ['models']
        if macro_paths is None:
            macro_paths = ['macros']
        if data_paths is None:
            data_paths = ['data']
        if test_paths is None:
            test_paths = ['test']
        if analysis_paths is None:
            analysis_paths = []
        if docs_paths is None:
            docs_paths = source_paths[:]
        if clean_targets is None:
            clean_targets = [target_path]
        if models is None:
            models = {}
        if on_run_start is None:
            on_run_start = []
        if on_run_end is None:
            on_run_end = []
        if archive is None:
            archive = {}

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
        try:
            typename = selected_profile.pop('type')
        except KeyError:
            raise dbt.exceptions.ValidationException(
                'required field "type" not found in profile {}'
                .format(profile_name))
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
            credentials=credentials)
