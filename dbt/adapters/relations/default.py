

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

    # Here for compatibility with old Relation interface
    @property
    def table(self):
        return self._table_name

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

    def quote(self, database=None, schema=None, identifier=None):
        raw_policy = {
            "database": database,
            "schema": schema,
            "identifier": identifier
        }

        policy_update = {k: v for (k,v) in raw_policy.items() if v is not None}
        policy = self.quoting.copy()
        policy.update(policy_update)

        return type(self)(
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

        return type(self)(
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

    @classmethod
    def create_from_node(cls, profile, node, **kwargs):
        return cls(
            database=profile['dbname'],
            schema=node['schema'],
            identifier=node['name'],
            **kwargs
        )

    @classmethod
    def create_from_parts(cls, database=None, schema=None, identifier=None):
        return cls(database=database, schema=schema, identifier=identifier)

    def __init__(self, database=None, schema=None, identifier=None,
                 table_name=None, quoting=None, include=None):

        self._database = database
        self._schema = schema
        self._identifier = identifier

        # This field is deprecated, but exists for backwards compatibility
        # with the existing implementation of Relations
        if table_name is None:
            self._table_name = identifier
        else:
            self._table_name = table_name

        self._quoting = self.QuotePolicy.copy()
        self._quoting.update(quoting or {})

        self._include = self.IncludePolicy.copy()
        self._include.update(include or {})

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.render())

    def __str__(self):
        return self.render()


if __name__ == '__main__':
    r = DefaultRelation("table", database='whatever', schema='ok', identifier='cool')
    r.render()
