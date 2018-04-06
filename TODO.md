- [ ] rip out query_for_existing > replace with get_relation
  - maybe part of the relation object is metadata about when the relation was pulled
    from the db (relation type, when we asked the db about it, columns, ??)
- [ ] add get_relation, list_relations
- fns / macros
  - [x] query_for_existing
  - [ ] get_columns_in_table
  - [ ] rename
  - [ ] macro: get_existing_relation_type
  - [ ] macro: create_table_as
