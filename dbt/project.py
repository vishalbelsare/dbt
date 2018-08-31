import os.path
import pprint
import copy
import hashlib
import re

import dbt.deprecations
import dbt.contracts.connection
import dbt.clients.yaml_helper
import dbt.clients.jinja
import dbt.compat
import dbt.context.common
import dbt.clients.system
import dbt.ui.printer
import dbt.links

from dbt.api.object import APIObject
from dbt.utils import deep_merge
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

default_project_cfg = {
    'source-paths': ['models'],
    'macro-paths': ['macros'],
    'data-paths': ['data'],
    'test-paths': ['test'],
    'target-path': 'target',
    'clean-targets': ['target'],
    'outputs': {'default': {}},
    'target': 'default',
    'models': {},
    'quoting': {},
    'profile': None,
    'packages': [],
    'modules-path': 'dbt_modules'
}

default_profiles = {}

default_profiles_dir = os.path.join(os.path.expanduser('~'), '.dbt')

NO_SUPPLIED_PROFILE_ERROR = """\
dbt cannot run because no profile was specified for this dbt project.
To specify a profile for this project, add a line like the this to
your dbt_project.yml file:

profile: [profile name]

Here, [profile name] should be replaced with a profile name
defined in your profiles.yml file. You can find profiles.yml here:

{profiles_file}/profiles.yml
""".format(profiles_file=default_profiles_dir)


class DbtProjectError(Exception):
    def __init__(self, message, project=None):
        self.project = project
        super(DbtProjectError, self).__init__(message)


class DbtProfileError(Exception):
    def __init__(self, message, project=None):
        super(DbtProfileError, self).__init__(message)


def read_profiles(profiles_dir=None):
    if profiles_dir is None:
        profiles_dir = default_profiles_dir

    raw_profiles = dbt.config.read_profile(profiles_dir)

    if raw_profiles is None:
        profiles = {}
    else:
        profiles = {k: v for (k, v) in raw_profiles.items() if k != 'config'}

    return profiles
