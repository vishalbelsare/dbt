from dbt.api.object import APIObject
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.contracts.graph.parsed

RELATION_SCHEMA_CONTRACT = {
    "type": "object",
    "description": "A parsed Schema specification for a db relation",
    "patternProperties": {
        ".*": {
            "type": "object",
            "properties": {
                "metadata": {
                    "type": "object",
                    "properties": {
                        "schema": {
                            "type": "string"
                        },
                        "name": {
                            "type": "string"
                        },
                        "type": {
                            "type": "string"
                        },
                        "comment": {
                            "type": ["null", "string"]
                        }
                    }
                },
                "columns": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string"
                            },
                            "index": {
                                "type": "integer"
                            },
                            "type": {
                                "type": "string"
                            },
                            "comment": {
                                "type": ["null", "string"]
                            },
                            "tests": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "test_name": {
                                            "type": "string"
                                        },
                                        "args": {
                                            "type": "object"
                                        },
                                        "namespace": {
                                            "type": ["null", "string"]
                                        },
                                        "model_name": {
                                            "type": "string"
                                        },
                                    },
                                    "required": ["test_name", "args", "namespace", "model_name"]
                                }
                            },
                            "required": ["name", "index", "type", "comment", "tests"],
                            "additionalProperties": False
                        }
                    }
                },
                "required": ["columns", "metadata"],
                "additionalProperties": False
            }
        },
        "additionalProperties": False
    },
    "additionalProperties": False,
}

CATALOG_CONTRACT = {
    "type": "object",
    "additionalProperties": False,
    "patternProperties": {
        ".*": RELATION_SCHEMA_CONTRACT
    },
    "required": [], # TODO
}

class RelationSpec(APIObject):
    SCHEMA = RELATION_SCHEMA_CONTRACT

class Catalog(APIObject):
    SCHEMA = CATALOG_CONTRACT
