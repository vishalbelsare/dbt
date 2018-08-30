import os.path
from copy import deepcopy

import dbt.exceptions
import dbt.clients.yaml_helper
import dbt.clients.system
from dbt.contracts.connection import Connection, create_credentials
from dbt.contracts.project import Project, Configuration, PackageConfig
from dbt.context.common import env_var
from dbt import compat
from dbt.project import DbtProjectError, DbtProfileError, \
    NO_SUPPLIED_PROFILE_ERROR

from dbt.logger import GLOBAL_LOGGER as logger


DEFAULT_THREADS = 1
DEFAULT_SEND_ANONYMOUS_USAGE_STATS = True
DEFAULT_USE_COLORS = True
DEFAULT_QUOTING_GLOBAL = {
    'identifier': True,
    'schema': True,
}
# some adapters need different quoting rules, for example snowflake gets a bit
# weird with quoting on
DEFAULT_QUOTING_ADAPTER = {
    'snowflake': {
        'identifier': False,
        'schema': False,
    },
}
DEFAULT_PROFILES_DIR = os.path.join(os.path.expanduser('~'), '.dbt')


INVALID_PROFILE_MESSAGE = """
dbt encountered an error while trying to read your profiles.yml file.

{error_string}
"""

NO_SUPPLIED_PROFILE_ERROR = """\
dbt cannot run because no profile was specified for this dbt project.
To specify a profile for this project, add a line like the this to
your dbt_project.yml file:

profile: [profile name]

Here, [profile name] should be replaced with a profile name
defined in your profiles.yml file. You can find profiles.yml here:

{profiles_file}/profiles.yml
""".format(profiles_file=DEFAULT_PROFILES_DIR)


class DbtProjectError(Exception):
    def __init__(self, message, project=None, result_type='invalid_project'):
        self.project = project
        super(DbtProjectError, self).__init__(message)


class DbtProfileError(Exception):
    def __init__(self, message, project=None, result_type='invalid_profile'):
        super(DbtProfileError, self).__init__(message)


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


def _render(value, ctx):
    if isinstance(value, compat.basestring):
        return dbt.clients.jinja.get_rendered(value, ctx)
    else:
        return value


class Project(object):
    def __init__(self, project_name, version, project_root, source_paths,
                 macro_paths, data_paths, test_paths, analysis_paths,
                 docs_paths, target_path, clean_targets, log_path,
                 modules_path, quoting, models, on_run_start, on_run_end,
                 archive):
        self.project_name = project_name
        self.version = version
        self.project_root = project_root
        self.source_paths = source_paths
        self.macro_paths = macro_paths
        self.data_paths = data_paths
        self.test_paths = test_paths
        self.analysis_paths = analysis_paths
        self.docs_paths = docs_paths
        self.target_path = target_path
        self.clean_targets = clean_targets
        self.log_path = log_path
        self.modules_path = modules_path
        self.quoting = quoting
        self.models = models
        self.on_run_start = on_run_start
        self.on_run_end = on_run_end
        self.archive = archive

    @classmethod
    def from_project_config(cls, project_dict):
        # just for validation.
        try:
            Project(**project_dict)
        except dbt.exceptions.ValidationException as e:
            raise DbtProjectError(str(e))

        # name/version are required in the Project definition, so we can assume
        # they are present
        name = project_dict['name']
        version = project_dict['version']
        source_paths = project_dict.get('source-paths', ['models'])
        macro_paths = project_dict.get('macro-paths', ['macros'])
        data_paths = project_dict.get('data-paths', ['data'])
        test_paths = project_dict.get('test-paths', ['test'])
        analysis_paths = project_dict.get('analysis-paths', [])
        docs_paths = project_dict.get('docs-paths', source_paths[:])
        target_path = project_dict.get('target-path', 'target')
        clean_targets = project_dict.get('clean-targets', [target_path])
        profile = project_dict.get('profile')
        log_path = project_dict.get('log-path', 'logs')
        modules_path = project_dict.get('modules-path', 'dbt_modules')
        # in the default case we'll populate this once we know the adapter type
        quoting = project_dict.get('quoting', {})
        models = project_dict.get('models', {})
        on_run_start = project_dict.get('on-run-start', [])
        on_run_end = project_dict.get('on-run-end', [])
        archive = project_dict.get('archive', {})

        return cls(
            project_name=name,
            version=version,
            project_root=project_root,
            source_paths=source_paths,
            macro_paths=macro_paths,
            data_paths=data_paths,
            test_paths=test_paths,
            analysis_paths=analysis_paths,
            docs_paths=docs_paths,
            target_path=target_path,
            clean_targets=clean_targets,
            log_path=log_path,
            modules_path=modules_path,
            quoting=quoting,
            models=models,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            archive=archive
        )

    def to_project_config(self):
        return deepcopy({
            'name': self.project_name,
            'version': self.version,
            'source-paths': self.source_paths,
            'macro-paths': self.macro_paths,
            'data-paths': self.data_paths,
            'test-paths': self.test_paths,
            'analysis-paths': self.analysis_paths,
            'docs-paths': self.docs_paths,
            'target-path': self.target_path,
            'clean-targets': self.clean_targets,
            'log-path': self.log_path,
            'quoting': self.quoting,
            'models': self.models,
            'on-run-start': self.on_run_start,
            'on-run-end': self.on_run_end,
            'archive': self.archive,
            'profile': self.profile_name,
            'project-root': self.project_root,
        })


class Profile(object):
    def __init__(self, profile_name, target_name, send_anonymous_usage_stats,
                 use_colors, threads, credentials):
        self.profile_name = profile_name
        self.target_name = target_name
        self.send_anonymous_usage_stats = send_anonymous_usage_stats
        self.use_colors = use_colors
        self.threads = threads
        self.credentials = credentials

    def to_profile_info(self):
        """Unlike to_project_config, this dict is not a mirror of any existing
        on-disk data structure. It's used when creating a new profile from an
        existing one.
        """
        return {
            'profile_name': self.profile_name,
            'target': self.target,
            'send_anonymous_usage_stats': self.send_anonymous_usage_stats,
            'use_colors': self.use_colors,
            'threads': self.threads,
            'credentials': self.credentials.incorporate(),
        }

    @staticmethod
    def _creds_from_profile(args, profile, profile_name, target_name):
        # if entries are strings, we want to render them so we can get any
        # environment variables that might store important credentials elements.
        credentials = {
            k: _render(v, {'env_var': env_var})
            for k, v in profile.items()
        }

        # valid connections never include the number of threads, but it's
        # stored on a per-connection level in the raw configs
        threads = credentials.pop('threads', DEFAULT_THREADS)
        if hasattr(args, 'threads') and args.threads is not None:
            threads = args.threads

        # credentials carry their 'type' in their actual type, not their
        # attributes. We do want this in order to pick our Credentials class.
        if 'type' not in credentials:
            raise DbtProjectError(
                'required field "type" not found in profile {} and target {}'
                .format(profile_name, target_name))

        typename = credentials.pop('type')
        try:
            credentials = create_credentials(typename, credentials)
        except dbt.exceptions.ValidationException as e:
            raise DbtProfileError(
                'Credentials in profile "{}" invalid: {}'
                .format(profile_name, str(e))
            )
        return credentials, threads

    @staticmethod
    def _pick_profile_name(args, profile_name=None):
        if args.profile is not None:
            profile_name = args.profile
        if profile_name is None:
            raise DbtProjectError(NO_SUPPLIED_PROFILE_ERROR)
        return profile_name

    @staticmethod
    def _pick_target(args, raw_profile):
        if args.target is not None:
            target_name = args.target
        elif 'target' in raw_profile:
            target_name = raw_profile['target']
        else:
            raise DbtProfileError(
                "target not specified in profile '{}'".format(profile_name)
            )
        return target_name

    @staticmethod
    def _get_profile_data(raw_profile, target_name):
        if 'outputs' not in raw_profile:
            raise DbtProfileError(
                "outputs not specified in profile '{}'".format(profile_name)
            )
        outputs = raw_profile['outputs']

        if target_name not in outputs:
            outputs = '\n'.join(' - {}'.format(output)
                                for output in outputs)
            msg = ("The profile '{}' does not have a target named '{}'. The "
                   "valid target names for this profile are:\n{}"
                   .format(profile_name, target, outputs))
            raise DbtProfileError(msg, result_type='invalid_target')
        profile_data = outputs[target_name]
        return profile_data

    @classmethod
    def from_args(cls, args, project_profile_name=None):
        """Given the raw profiles as read from disk and the name of the desired
        profile if specified, return the profile component of the runtime
        config.
        """
        profiles_dir = getattr(args, 'profiles_dir', DEFAULT_PROFILES_DIR)
        raw_profiles = read_profile(profiles_dir)

        profile_name = cls._pick_profile_name(args, project_profile_name)
        if profile_name not in raw_profiles:
            raise DbtProjectError(
                "Could not find profile named '{}'".format(profile_name)
            )
        raw_profile = raw_profiles[profile_name]

        target_name = cls._pick_target(args, raw_profile)
        profile_data = cls._get_profile_data(raw_profile, target_name)

        user_cfg = raw_profiles.get('config', {})
        send_anonymous_usage_stats = user_cfg.get(
            'send_anonymous_usage_stats',
            DEFAULT_SEND_ANONYMOUS_USAGE_STATS
        )
        use_colors = user_cfg.get(
            'use_colors',
            DEFAULT_USE_COLORS
        )

        credentials, threads = cls._creds_from_profile(
            args, profile_data, profile_name, target_name
        )

        return cls(
            profile_name=profile_name,
            target_name=target_name,
            send_anonymous_usage_stats=send_anonymous_usage_stats,
            use_colors=use_colors,
            threads=threads,
            credentials=credentials
        )


class RuntimeConfig(Project, Profile):
    """The runtime configuration, as constructed from its components. There's a
    lot because there is a lot of stuff!
    TODO:
        - make credentials/threads optional for some commands (dbt deps should not care)
            - via subclassing/superclassing, probably
    """
    def __init__(self, project_name, version, project_root, source_paths,
                 macro_paths, data_paths, test_paths, analysis_paths,
                 docs_paths, target_path, clean_targets, log_path,
                 modules_path, quoting, models, on_run_start, on_run_end,
                 archive, profile_name, target_name,
                 send_anonymous_usage_stats, use_colors, threads, credentials,
                 packages):
        # 'project'
        Project.__init__(
            self,
            project_name, version, project_root, source_paths, macro_paths,
            data_paths, test_paths, analysis_paths, docs_paths, target_path,
            clean_targets, log_path, modules_path, quoting, models,
            on_run_start, on_run_end, archive
        )
        # 'profile'
        Profile.__init__(
            self,
            profile_name, target_name, send_anonymous_usage_stats, use_colors,
            threads, credentials
        )
        # 'package'
        self.packages = packages
        self.validate()

    @classmethod
    def from_project_config_and_profile_info(cls, project_dict, profile_info, packages):
        project = Project.from_project_config(project_dict)

        quoting = deepcopy(
            DEFAULT_QUOTING_ADAPTER.get(profile_info['credentials'].type(),
                                        DEFAULT_QUOTING_GLOBAL)
        )
        quoting.update(project.quoting)

        return cls(
            project_name=project.name,
            version=project.version,
            project_root=project.project_root,
            source_paths=project.source_paths,
            macro_paths=project.macro_paths,
            data_paths=project.data_paths,
            test_paths=project.test_paths,
            analysis_paths=project.analysis_paths,
            docs_paths=project.docs_paths,
            target_path=project.target_path,
            clean_targets=project.clean_targets,
            log_path=project.log_path,
            modules_path=project.modules_path,
            quoting=quoting,
            models=project.models,
            on_run_start=project.on_run_start,
            on_run_end=project.on_run_end,
            archive=project.archive,
            packages=packages,
            **profile_info
        )

    @classmethod
    def from_project_config(cls, args, project_dict, packages_dict=None):
        """Create a RuntimeConfig from a dbt_project.yml file's configuration
        contents and the command-line arguments.
        """
        if packages_dict is None:
            packages_dict = {'packages': []}
        # the only thing we need from the profile info for this is the profile
        # field, which may be empty.
        profile_info = Profile.from_args(args, project_dict.get('profile'))
        try:
            packages = PackageConfig(**packages_dict)
        except dbt.exceptions.ValidationException as e:
            raise DbtProfileError('Invalid package config: {}'.format(str(e)))

        return cls.from_project_config_and_profile_info(
            project_dict=project_dict,
            packages=packages,
            # profile data
            profile_info=profile_info,
        )

    def new_project(self, project_root):
        """Given a new project root, read in its project dictionary, supply the
        existing project's profile info, and create a new project file.
        """
        packages = self.packages.incorporate()
        profile_info = self.to_profile_info()
        raise NotImplementedError

    def serialize(self):
        result = self.to_project_config()
        result.update(self.to_profile_info())
        result.update(self.packages.serialize())
        return result

    def validate(self):
        try:
            Configuration(**self.serialize())
        except dbt.exceptions.ValidationException as e:
            raise DbtProjectError(str(e))

    @classmethod
    def from_args(cls, args):
        """Given arguments, read in dbt_project.yml from the current directory,
        read in packages.yml if it exists, and use them to find the profile to
        load.
        """
        project_yaml_filepath = os.path.abspath('dbt_project.yml')
        project_dir = os.path.dirname(project_yaml_filepath)
        package_filepath = dbt.clients.system.resolve_path_from_base(
                'packages.yml', project_dir)

        # get the project.yml contents
        if not dbt.clients.system.path_exists(project_yaml_filepath):
            raise DbtProjectError(
                'no dbt_project.yml found at expected path {}'
                .format(project_yaml_filepath)
            )

        project_dict = _load_yaml(project_yaml_filepath)
        packages_dict = {'packages': []}
        if dbt.clients.system.path_exists(package_filepath):
            packages_dict = _load_yaml(package_filepath)
        return cls.from_project_config(args, project_dir, project_dict,
                                       packages_dict)

    def hashed_name(self):
        return hashlib.md5(self.project_name.encode('utf-8')).hexdigest()


def _load_yaml(path):
    contents = dbt.clients.system.load_file_contents(path)
    return dbt.clients.yaml_helper.load_yaml_text(contents)
