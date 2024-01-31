# Represents the metadata of an entity.
#
# + entityName - Name of the entity
# + collectionName - Collection name of the entity
# + fieldMetadata - Metadata of all the fields of the entity
# + keyFields - Names of the identity fields
public type RedisMetadata record {|
    string entityName;
    string collectionName;
    map<FieldMetadata> fieldMetadata;
    string[] keyFields;
|};

# Represents the metadata associated with a field from a related entity.
#
# + relation - The relational metadata associated with the field
public type EntityFieldMetadata record {|
    RelationMetadata relation;
|};

# Represents the metadata associated with a simple field in the entity record.
#
# + fieldName - The name of the Redis document field to which the object field is mapped
# + fieldDataType - The data type of the object field to which the Redis document field mapped
public type SimpleFieldMetadata record {|
    string fieldName;
    DataType fieldDataType;
|};

# Represents the metadata associated with a field of an entity.
# Only used by the generated persist clients and `persist:RQLClient`.
#
public type FieldMetadata SimpleFieldMetadata|EntityFieldMetadata;

# Represents the metadata associated with a relation.
# Only used by the generated persist clients and `persist:RQLClient`.
#
# + entityName - The name of the entity represented in the relation  
# + refField - The name of the referenced field in the Redis document
# + refFieldDataType - The data type of the object field to which the referenced field in 
# Redis document is mapped

public type RelationMetadata record {|
    string entityName;
    string refField;
    DataType refFieldDataType;
|};

# Represents the type of the relation used in a `JOIN` operation.
# Only used by the generated persist clients and `rql:SQLClient`.
#
# + ONE_TO_ONE - The association type is a one-to-one association
# + ONE_TO_MANY - The entity is in the 'one' side of a one-to-many association
# + MANY_TO_ONE - The entity is in the 'many' side of a one-to-many association
public enum JoinType {
    ONE_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_ONE
}


# Represents the type of the field data.
# Only used by the generated persist clients and `rql:RQLClient`.
#
# + INT - `int` type
# + STRING - `string` type
# + FLOAT - `float` type
# + BOOLEAN - `boolean` type
# + TIME - `time/date` type
public enum DataType {
    INT,
    STRING,
    FLOAT,
    BOOLEAN,
    TIME,
    ENUM
}

# Represents the types of Metadata a RQL client can hold.
# Only used by the generated persist clients and `rql:RQLClient`.
#
# + FIELD_DATA_TYPE - `int` type
# + RELATION - `string` type
# + REF_FIELD_DATA_TYPE - `float` type
public enum MetaData {
    FIELD_DATA_TYPE = "fieldDataType",
    RELATION = "relation",
    REF_FIELD_DATA_TYPE = "refFieldDataType"
}