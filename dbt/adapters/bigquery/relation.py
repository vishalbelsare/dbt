from dbt.adapters.default.relation import DefaultRelation
from dbt.utils import filter_null_values


class BigQueryRelation(DefaultRelation):

    DEFAULTS = {
        'metadata': {
            '_type': 'DefaultRelation'
        },
        'quote_character': '"',
        'quote_policy': {
            'project': True,
            'dataset': True,
            'identifier': True
        },
        'include_policy': {
            'project': False,
            'dataset': True,
            'identifier': True
        }
    }

    PATH_DATASET = {
        'type': 'object',
        'properties': {
            'project': {'type': ['string', 'null']},
            'dataset': {'type': ['string', 'null']},
            'identifier': {'type': 'string'},
        },
        'required': ['project', 'dataset', 'identifier'],
    }

    POLICY_DATASET = {
        'type': 'object',
        'properties': {
            'project': {'type': 'boolean'},
            'dataset': {'type': 'boolean'},
            'identifier': {'type': 'boolean'},
        },
        'required': ['project', 'dataset', 'identifier'],
    }

    def matches(self, project=None, dataset=None, identifier=None):
        search = filter_null_values({
            'project': project,
            'dataset': dataset,
            'identifier': identifier
        })

        if not search:
            # nothing was passed in
            pass

        for k, v in search.items():
            if self.get_path_part(k) != v:
                return False

        return True

    def quote(self, project=None, dataset=None, identifier=None):
        policy = filter_null_values({
            'project': project,
            'dataset': dataset,
            'identifier': identifier
        })

        return self.incorporate(quote_policy=policy)

    def include(self, project=None, dataset=None, identifier=None):
        policy = filter_null_values({
            'project': project,
            'dataset': dataset,
            'identifier': identifier
        })

        return self.incorporate(include_policy=policy)
