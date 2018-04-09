from dbt.adapters.default.relation import DefaultRelation
from dbt.utils import filter_null_values


class SnowflakeRelation(DefaultRelation):
    DEFAULTS = {
        'metadata': {
            '_type': 'DefaultRelation'
        },
        'quote_character': '"',
        'quote_policy': {
            'database': False,
            'schema': False,
            'identifier': False,
        },
        'include_policy': {
            'database': False,
            'schema': True,
            'identifier': True,
        }
    }

    @classmethod
    def create_from_node(cls, profile, node, **kwargs):
        return cls.create(
            database=profile.get('database'),
            schema=node.get('schema'),
            identifier=node.get('name'),
            **kwargs)

    def matches(self, database=None, schema=None, identifier=None):
        search = filter_null_values({
            'database': database,
            'schema': schema,
            'identifier': identifier
        })

        if not search:
            # nothing was passed in
            pass

        for k, v in search.items():
            # snowflake upcases unquoted identiifers. so, when
            # comparing unquoted identifiers, use case insensitive
            # matching. when comparing quoted identifiers, use case
            # sensitive matching.
            if self.should_quote(k):
                if self.get_path_part(k) != v:
                    return False

            else:
                if self.get_path_part(k) != v.upper():
                    return False

        return True

    def get_path_part(self, part):
        return self.path.get(part)
