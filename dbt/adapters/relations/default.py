

# This should implement schema/name props for compatibility with old relation obj
class DefaultRelation(object):
    QuoteCharacter = '"'
    QuotePolicy = {
        "database": True,
        "schema": True,
        "identifier": True
    }

    IncludePolicy = {
        "database": False,
        "schema": True,
        "identifier": True
    }

    Table = "table"
    View = "view"

    RelationTypes = [
        Table,
        View
    ]

    @property
    def database(self):
        return self._database

    @property
    def schema(self):
        return self._schema

    @property
    def identifier(self):
        return self._identifier

    # Here for compatibility with old Relation interface
    @property
    def name(self):
        return self.identifier

    def should_quote(self, part):
        return self._quoting.get(part)

    def should_include(self, part):
        return self._include.get(part)

    @property
    def inclusion(self):
        return self._include

    @property
    def quoting(self):
        return self._quoting

    @classmethod
    def create_from_node(cls, profile, adapter, node):
        import ipdb; ipdb.set_trace()
        pass

    @classmethod
    def create_from_parts(cls, database=None, schema=None, identifier=None):
        return cls(database=database, schema=schema, identifier=identifier)

    def quote(self, database=None, schema=None, identifier=None):
        raw_policy = {
            "database": database,
            "schema": schema,
            "identifier": identifier
        }

        policy_update = {k: v for (k,v) in raw_policy.items() if v is not None}
        policy = self.quoting.copy()
        policy.update(policy_update)

        return type(self)(self._type,
                   database=self.database,
                   schema=self.schema,
                   identifier=self.identifier,
                   quoting=policy,
                   include=self.inclusion)

    def include(self, database=None, schema=None, identifier=None):
        raw_policy = {
            "database": database,
            "schema": schema,
            "identifier": identifier
        }

        policy_update = {k: v for (k,v) in raw_policy.items() if v is not None}
        policy = self.inclusion.copy()
        policy.update(policy_update)

        return type(self)(self._type,
                   database=self.database,
                   schema=self.schema,
                   identifier=self.identifier,
                   quoting=self.quoting,
                   include=policy)

    def render(self):
        parts = []

        if self.database is not None and self.should_include('database'):
            parts.append(self.quote_if(self.database, self.should_quote('database')))

        if self.schema is not None and self.should_include('schema'):
            parts.append(self.quote_if(self.schema, self.should_quote('schema')))

        if self.identifier is not None and self.should_include('identifier'):
            parts.append(self.quote_if(self.identifier, self.should_quote('identifier')))


        if len(parts) == 0:
            # TODO
            raise RuntimeError("Nothing to quote here....")

        return '.'.join(parts)

    def quote_if(self, identifier, should_quote):
        if should_quote:
            return self.quoted(identifier)

        return identifier

    @classmethod
    def quoted(cls, s):
        return '{quote_char}{identifier}{quote_char}'.format(
                    quote_char=cls.QuoteCharacter, identifier=s)

    def __init__(self, relation_type=None, database=None, schema=None,
                 identifier=None, quoting=None, include=None):

        if relation_type not in DefaultRelation.RelationTypes:
            # TODO - compiler error
            raise RuntimeError("Relation Type {} is invalid".format(
                relation_type))

        self._type = relation_type

        self._database = database
        self._schema = schema
        self._identifier = identifier
        self._type = relation_type

        self._quoting = self.QuotePolicy.copy()
        self._quoting.update(quoting or {})

        self._include = self.IncludePolicy.copy()
        self._include.update(include or {})

    def __repr__(self):
        return "<{} {}: {}>".format(self.__class__.__name__, self._type, self.render())

    def __str__(self):
        return self.render()


if __name__ == '__main__':
    r = DefaultRelation("table", database='whatever', schema='ok', identifier='cool')
    r.render()
