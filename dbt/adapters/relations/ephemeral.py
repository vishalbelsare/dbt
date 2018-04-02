

from dbt.adapters.relations.default import DefaultRelation


class EphemeralRelation(DefaultRelation):
    def render(self):
        return '__dbt__CTE__{}'.format(self.identifier)
