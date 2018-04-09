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

    PATH_SCHEMA = {
        'type': 'object',
        'properties': {
            'project': {'type': ['string', 'null']},
            'dataset': {'type': ['string', 'null']},
            'identifier': {'type': 'string'},
        },
        'required': ['project', 'dataset', 'identifier'],
    }

    POLICY_SCHEMA = {
        'type': 'object',
        'properties': {
            'project': {'type': 'boolean'},
            'dataset': {'type': 'boolean'},
            'identifier': {'type': 'boolean'},
        },
        'required': ['project', 'dataset', 'identifier'],
    }

    SCHEMA = {
        'type': 'object',
        'properties': {
            'metadata': {
                '_type': {
                    'type': 'string',
                    'const': 'DefaultRelation',
                },
            },
            'type': {
                'enum': DefaultRelation.RelationTypes + [None],
            },
            'path': PATH_SCHEMA,
            'include_policy': POLICY_SCHEMA,
            'quote_policy': POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }

    PATH_ELEMENTS = ['project', 'dataset', 'identifier']

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
