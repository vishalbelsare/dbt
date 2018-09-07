"""Unit test utility functions.

Note that all imports should be inside the functions to avoid import/mocking
issues.
"""

def config_from_parts_or_dicts(project, profile, packages=None, cli_vars='{}'):
    from dbt.config import Project, Profile, RuntimeConfig
    from dbt.utils import parse_cli_vars
    from copy import deepcopy
    if not isinstance(project, Project):
        project = Project.from_project_config(deepcopy(project), packages)
    if not isinstance(profile, Profile):
        profile = Profile.from_raw_profile_info(deepcopy(profile),
                                                project.profile_name)
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    return RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        cli_vars=cli_vars
    )
