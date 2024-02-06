// Copyright (c) 2023 WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

# Represents the metadata of an entity.
#
# + entityName - Name of the entity
# + collectionName - Collection name of the entity
# + fieldMetadata - Metadata of all the fields of the entity
# + keyFields - Names of the identity fields
# + refMetadata - Metadata of the fields that is being reffered
public type RedisMetadata record {|
    string entityName;
    string collectionName;
    map<FieldMetadata> fieldMetadata;
    string[] keyFields;
    map<RefMetadata> refMetadata?;
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
# Only used by the generated persist clients and `persist:RedisClient`.
#
public type FieldMetadata SimpleFieldMetadata|EntityFieldMetadata;

# Represents the metadata associated with a relation.
# Only used by the generated persist clients and `persist:RedisClient`.
#
# + entityName - The name of the entity represented in the relation  
# + refField - The name of the refered field in the Redis document
# + refFieldDataType - The data type of the object field to which the refered field in 
# Redis document is mapped

public type RelationMetadata record {|
    string entityName;
    string refField;
    DataType refFieldDataType;
|};

# Represents the metadata associated with relations
# Only used by the generated persist clients and `persist:RedisClient`.
#
# + entity - The name of the entity that is being joined  
# + fieldName - The name of the field in the `entity` that is being joined  
# + refCollection - The name of the Redis collection to be joined  
# + refFields - The names of the fields of the refered collection
# + joinFields - The names of the join fields
# + joinCollection - The name of the joining collection used for a many-to-many relation
# + joiningRefFields - The names of the refered fields in the joining collection     
# + joiningJoinFields - The names of the join fields in the joining collection     
# + 'type - The type of the relation
public type RefMetadata record {|
    typedesc<record {}> entity;
    string fieldName;
    string refCollection;
    string[] refFields;
    string[] joinFields;
    string joinCollection?;
    string[] joiningRefFields?;
    string[] joiningJoinFields?;
    CardinalityType 'type;
|};

# Represents the cardinality of the relationship
# Only used by the generated persist clients and `persist:RedisClient`.
#
# + ONE_TO_ONE - The association type is a one-to-one association
# + ONE_TO_MANY - The entity is in the 'one' side of a one-to-many association
# + MANY_TO_ONE - The entity is in the 'many' side of a one-to-many association
public enum CardinalityType {
    ONE_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_ONE
}

# Represents the type of the field data.
# Only used by the generated persist clients and `persist:RedisClient`.
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

# Represents the type of the redis supported data structures.
# Only used by the generated persist clients and `persist:RedisClient`.
#
# + REDIS_STRING - `string` type
# + REDIS_SET - `set` type
# + REDIS_HASH - `hash` type
public enum RedisDataType {
    REDIS_STRING = "string",
    REDIS_SET = "set",
    REDIS_HASH = "hash"
}

# Represents the types of Metadata in a `persist:RedisClient`.
# Only used by the generated persist clients and `persist:RedisClient`.
#
# + FIELD_DATA_TYPE - `int` type
# + RELATION - `string` type
# + REF_FIELD_DATA_TYPE - `float` type
public enum MetaData {
    FIELD_DATA_TYPE = "fieldDataType",
    RELATION = "relation",
    REF_FIELD_DATA_TYPE = "refFieldDataType"
}
