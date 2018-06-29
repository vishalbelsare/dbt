
from dbt.api import APIObject

# input.py is prolly the wrong name for this

UNPARSED_SCHEMA_SPEC_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    "properties": {
        "name": {
            "type": "string",
        },
        "root_path": {
            "type": "string"
        },
        #"resource_type": {
        #    "type": "string"
        #},
        "path": {
            "type": "string"
        },
        "original_file_path": {
            "type": "string"
        },
        "package_name": {
            "type": "string"
        },
        "raw_yml": {
            "type": "string"
        }
    },
    "required": ["name", "root_path", "path", "original_file_path", "package_name", "raw_yml"] # resource_type?
}

RAW_SCHEMA_SPEC_V1_CONTRACT = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "source": UNPARSED_SCHEMA_SPEC_CONTRACT,
        "data": {
            "patternProperties": {
                ".*": {  # Model name
                    'type': 'object',
                    'properties': {
                        'constraints': {
                            'type': 'object',
                            'patternProperties': {  # Test name
                                '.*': {
                                    'type': ['array'],
                                    'items': {}
                                }
                            }
                        },
                        'required': 'constraints'
                    }
                }
            }
        },
        'required': ['source', 'data']
    }
}


RAW_SCHEMA_SPEC_V2_CONTRACT = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "source": UNPARSED_SCHEMA_SPEC_CONTRACT,
        "data": {
            'type': 'object',
            'additionalProperties': False,
            "properties": {
                "version": {
                    "type": "integer"
                },
                "sources": {
                    "type": "array" # TODO
                },
                "models": {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'additionalProperties': False,
                        'properties': {
                            'name': {
                                'type': 'string'
                            },
                            'description': {
                                'type': ['null', 'string']
                            },
                            'options': { # optional
                                'type': 'object',
                            },
                            'columns': {
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'additionalProperties': False,
                                        'name': {
                                            'type': 'string'
                                        },
                                        'description': {
                                            'type': ['null', 'string']
                                        },
                                        'tests': {
                                            'type': 'array',
                                            'items': {
                                                'type': ['string', 'object'],
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    'required': ['source', 'data']
}


PARSED_SCHEMA_SPEC = {
    "type": "object",
    "additionalProperties": False,
    "patternProperties": {
        ".*": {
            'type': 'object',
            'properties': {
                'source': UNPARSED_SCHEMA_SPEC_CONTRACT,
                'name': {
                    'type': 'string'
                },
                "options": {
                    'type': ['object']
                },
                'description': {
                    'type': ['null', 'string']
                },
                'columns': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'name': {
                                'type': 'string'
                            },
                            'description': {
                                'type': ['null', 'string']
                            },
                            # This is the only difference between v2 spec and SchemaSpec
                            'tests': {
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'test_name': {
                                            'type': 'string',
                                        },
                                        'test_config': {
                                            'type': ['object', 'string']
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    'required': [] # TODO
}

SCHEMA_SPEC_COLLECTION = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': {
            'type': 'object',
            'properties': PARSED_SCHEMA_SPEC
        }
    }
}

PARSED_SOURCE_SPEC = {
    "type": "object",
    "additionalProperties": False,
    "patternProperties": {
        ".*": {
            'type': 'object',
            'properties': {
                'source': UNPARSED_SCHEMA_SPEC_CONTRACT,
                'parent': {
                    'type': 'object',
                    'properties': {
                        'name': {
                            'type': 'string',
                        },
                        'description': {
                            'type': ['null', 'string']
                        }
                    }
                },
                'name': {
                    'type': 'string'
                },
                'sql_table_name': {
                    'type': 'string'
                },
                "options": {
                    'type': ['object']
                },
                'description': {
                    'type': ['null', 'string']
                },
                'columns': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'name': {
                                'type': 'string'
                            },
                            'description': {
                                'type': ['null', 'string']
                            },
                            # This is the only difference between v2 spec and SchemaSpec
                            'tests': {
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'test_name': {
                                            'type': 'string',
                                        },
                                        'test_config': {
                                            'type': ['object', 'string']
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    'required': [] # TODO
}


class RawSchemaSpecV1(APIObject):
    SCHEMA = RAW_SCHEMA_SPEC_V1_CONTRACT


class RawSchemaSpecV2(APIObject):
    SCHEMA = RAW_SCHEMA_SPEC_V2_CONTRACT


class ParsedSchemaSpec(APIObject):
    SCHEMA = PARSED_SCHEMA_SPEC


class ParsedSourceSpec(APIObject):
    SCHEMA = PARSED_SOURCE_SPEC


class SchemaSpecCollection(APIObject):
    SCHEMA = SCHEMA_SPEC_COLLECTION

    def get_description_for_model(self, model_name):
        models = self.serialize()
        return models.get(model_name, {}).get('description')
