// Copyright (c) 2024 WSO2 LLC. (http://www.wso2.com) All Rights Reserved.
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
import ballerina/jballerina.java;
import ballerina/persist;
import ballerinax/redis;

const ALL_TYPES = "alltypes";
const STRING_ID_RECORD = "stringidrecords";
const INT_ID_RECORD = "intidrecords";
const FLOAT_ID_RECORD = "floatidrecords";
const DECIMAL_ID_RECORD = "decimalidrecords";
const BOOLEAN_ID_RECORD = "booleanidrecords";
const COMPOSITE_ASSOCIATION_RECORD = "compositeassociationrecords";
const ALL_TYPES_ID_RECORD = "alltypesidrecords";

public isolated client class RedisTestEntitiesClient {
    *persist:AbstractPersistClient;

    private final redis:Client dbClient;

    private final map<RedisClient> persistClients;

    private final record {|RedisMetadata...;|} & readonly metadata = {
        [ALL_TYPES]: {
            entityName: "AllTypes",
            collectionName: "AllTypes",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: INT},
                booleanType: {fieldName: "booleanType", fieldDataType: BOOLEAN},
                intType: {fieldName: "intType", fieldDataType: INT},
                floatType: {fieldName: "floatType", fieldDataType: FLOAT},
                decimalType: {fieldName: "decimalType", fieldDataType: DECIMAL},
                stringType: {fieldName: "stringType", fieldDataType: STRING},
                dateType: {fieldName: "dateType", fieldDataType: DATE},
                timeOfDayType: {fieldName: "timeOfDayType", fieldDataType: TIME_OF_DAY},
                utcType: {fieldName: "utcType", fieldDataType: UTC},
                civilType: {fieldName: "civilType", fieldDataType: CIVIL},
                booleanTypeOptional: {fieldName: "booleanTypeOptional", fieldDataType: BOOLEAN},
                intTypeOptional: {fieldName: "intTypeOptional", fieldDataType: INT},
                floatTypeOptional: {fieldName: "floatTypeOptional", fieldDataType: FLOAT},
                decimalTypeOptional: {fieldName: "decimalTypeOptional", fieldDataType: DECIMAL},
                stringTypeOptional: {fieldName: "stringTypeOptional", fieldDataType: STRING},
                dateTypeOptional: {fieldName: "dateTypeOptional", fieldDataType: DATE},
                timeOfDayTypeOptional: {fieldName: "timeOfDayTypeOptional", fieldDataType: TIME_OF_DAY},
                utcTypeOptional: {fieldName: "utcTypeOptional", fieldDataType: UTC},
                civilTypeOptional: {fieldName: "civilTypeOptional", fieldDataType: CIVIL},
                enumType: {fieldName: "enumType", fieldDataType: ENUM},
                enumTypeOptional: {fieldName: "enumTypeOptional", fieldDataType: ENUM}
            },
            keyFields: ["id"]
        },
        [STRING_ID_RECORD]: {
            entityName: "StringIdRecord",
            collectionName: "StringIdRecord",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: STRING},
                randomField: {fieldName: "randomField", fieldDataType: STRING}
            },
            keyFields: ["id"]
        },
        [INT_ID_RECORD]: {
            entityName: "IntIdRecord",
            collectionName: "IntIdRecord",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: INT},
                randomField: {fieldName: "randomField", fieldDataType: STRING}
            },
            keyFields: ["id"]
        },
        [FLOAT_ID_RECORD]: {
            entityName: "FloatIdRecord",
            collectionName: "FloatIdRecord",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: FLOAT},
                randomField: {fieldName: "randomField", fieldDataType: STRING}
            },
            keyFields: ["id"]
        },
        [DECIMAL_ID_RECORD]: {
            entityName: "DecimalIdRecord",
            collectionName: "DecimalIdRecord",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: DECIMAL},
                randomField: {fieldName: "randomField", fieldDataType: STRING}
            },
            keyFields: ["id"]
        },
        [BOOLEAN_ID_RECORD]: {
            entityName: "BooleanIdRecord",
            collectionName: "BooleanIdRecord",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: BOOLEAN},
                randomField: {fieldName: "randomField", fieldDataType: STRING}
            },
            keyFields: ["id"]
        },
        [COMPOSITE_ASSOCIATION_RECORD]: {
            entityName: "CompositeAssociationRecord",
            collectionName: "CompositeAssociationRecord",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: STRING},
                randomField: {fieldName: "randomField", fieldDataType: STRING},
                alltypesidrecordBooleanType: {fieldName: "alltypesidrecordBooleanType", fieldDataType: BOOLEAN},
                alltypesidrecordIntType: {fieldName: "alltypesidrecordIntType", fieldDataType: INT},
                alltypesidrecordFloatType: {fieldName: "alltypesidrecordFloatType", fieldDataType: FLOAT},
                alltypesidrecordDecimalType: {fieldName: "alltypesidrecordDecimalType", fieldDataType: DECIMAL},
                alltypesidrecordStringType: {fieldName: "alltypesidrecordStringType", fieldDataType: STRING},
                "allTypesIdRecord.booleanType": {
                    relation: {
                        entityName: "allTypesIdRecord",
                        refField: "booleanType",
                        refFieldDataType: BOOLEAN
                    }
                },
                "allTypesIdRecord.intType": {
                    relation: {
                        entityName: "allTypesIdRecord",
                        refField: "intType",
                        refFieldDataType: INT
                    }
                },
                "allTypesIdRecord.floatType": {
                    relation: {
                        entityName: "allTypesIdRecord",
                        refField: "floatType",
                        refFieldDataType: FLOAT
                    }
                },
                "allTypesIdRecord.decimalType": {
                    relation: {
                        entityName: "allTypesIdRecord",
                        refField: "decimalType",
                        refFieldDataType: DECIMAL
                    }
                },
                "allTypesIdRecord.stringType": {
                    relation: {
                        entityName: "allTypesIdRecord",
                        refField: "stringType",
                        refFieldDataType: STRING
                    }
                },
                "allTypesIdRecord.randomField": {
                    relation: {
                        entityName: "allTypesIdRecord",
                        refField: "randomField",
                        refFieldDataType: STRING
                    }
                }
            },
            keyFields: ["id"],
            refMetadata: {
                allTypesIdRecord: {
                    entity: AllTypesIdRecord,
                    fieldName: "allTypesIdRecord",
                    refCollection: "AllTypesIdRecord",
                    refMetaDataKey: "compositeAssociationRecord",
                    refFields: [
                        "booleanType",
                        "intType",
                        "floatType",
                        "decimalType",
                        "stringType"
                    ],
                    joinFields: [
                        "alltypesidrecordBooleanType",
                        "alltypesidrecordIntType",
                        "alltypesidrecordFloatType",
                        "alltypesidrecordDecimalType",
                        "alltypesidrecordStringType"
                    ],
                    'type: ONE_TO_ONE
                }
            }
        },
        [ALL_TYPES_ID_RECORD]: {
            entityName: "AllTypesIdRecord",
            collectionName: "AllTypesIdRecord",
            fieldMetadata: {
                booleanType: {fieldName: "booleanType", fieldDataType: BOOLEAN},
                intType: {fieldName: "intType", fieldDataType: INT},
                floatType: {fieldName: "floatType", fieldDataType: FLOAT},
                decimalType: {fieldName: "decimalType", fieldDataType: DECIMAL},
                stringType: {fieldName: "stringType", fieldDataType: STRING},
                randomField: {fieldName: "randomField", fieldDataType: STRING},
                "compositeAssociationRecord.id": {
                    relation: {
                        entityName: "compositeAssociationRecord",
                        refField: "id",
                        refFieldDataType: STRING
                    }
                },
                "compositeAssociationRecord.randomField": {
                    relation: {
                        entityName: "compositeAssociationRecord",
                        refField: "randomField",
                        refFieldDataType: STRING
                    }
                },
                "compositeAssociationRecord.alltypesidrecordBooleanType": {
                    relation: {
                        entityName: "compositeAssociationRecord",
                        refField: "alltypesidrecordBooleanType",
                        refFieldDataType: BOOLEAN
                    }
                },
                "compositeAssociationRecord.alltypesidrecordIntType": {
                    relation: {
                        entityName: "compositeAssociationRecord",
                        refField: "alltypesidrecordIntType",
                        refFieldDataType: INT
                    }
                },
                "compositeAssociationRecord.alltypesidrecordFloatType": {
                    relation: {
                        entityName: "compositeAssociationRecord",
                        refField: "alltypesidrecordFloatType",
                        refFieldDataType: FLOAT
                    }
                },
                "compositeAssociationRecord.alltypesidrecordDecimalType": {
                    relation: {
                        entityName: "compositeAssociationRecord",
                        refField: "alltypesidrecordDecimalType",
                        refFieldDataType: DECIMAL
                    }
                },
                "compositeAssociationRecord.alltypesidrecordStringType": {
                    relation: {
                        entityName: "compositeAssociationRecord",
                        refField: "alltypesidrecordStringType",
                        refFieldDataType: STRING
                    }
                }
            },
            keyFields: ["booleanType", "intType", "floatType", "decimalType", "stringType"],
            refMetadata: {
                compositeAssociationRecord: {
                    entity: CompositeAssociationRecord,
                    fieldName: "compositeAssociationRecord",
                    refCollection: "CompositeAssociationRecord",
                    refFields: [
                        "alltypesidrecordBooleanType",
                        "alltypesidrecordIntType",
                        "alltypesidrecordFloatType",
                        "alltypesidrecordDecimalType",
                        "alltypesidrecordStringType"
                    ],
                    joinFields: [
                        "booleanType",
                        "intType",
                        "floatType",
                        "decimalType",
                        "stringType"
                    ],
                    'type: ONE_TO_ONE
                }
            }
        }
    };

    public isolated function init() returns persist:Error? {
        redis:Client|error dbClient = new (redis);
        if dbClient is error {
            return <persist:Error>error(dbClient.message());
        }
        self.dbClient = dbClient;
        self.persistClients = {
            [ALL_TYPES]: check new (dbClient, self.metadata.get(ALL_TYPES)),
            [STRING_ID_RECORD]: check new (dbClient, self.metadata.get(STRING_ID_RECORD)),
            [INT_ID_RECORD]: check new (dbClient, self.metadata.get(INT_ID_RECORD)),
            [FLOAT_ID_RECORD]: check new (dbClient, self.metadata.get(FLOAT_ID_RECORD)),
            [DECIMAL_ID_RECORD]: check new (dbClient, self.metadata.get(DECIMAL_ID_RECORD)),
            [BOOLEAN_ID_RECORD]: check new (dbClient, self.metadata.get(BOOLEAN_ID_RECORD)),
            [COMPOSITE_ASSOCIATION_RECORD]: check new (dbClient, self.metadata.get(COMPOSITE_ASSOCIATION_RECORD)),
            [ALL_TYPES_ID_RECORD]: check new (dbClient, self.metadata.get(ALL_TYPES_ID_RECORD))
        };
    }

    isolated resource function get alltypes(AllTypesTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get alltypes/[int id](AllTypesTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post alltypes(AllTypesInsert[] data) returns int[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ALL_TYPES);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from AllTypesInsert inserted in data
            select inserted.id;
    }

    isolated resource function put alltypes/[int id](AllTypesUpdate value) returns AllTypes|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ALL_TYPES);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/alltypes/[id];
    }

    isolated resource function delete alltypes/[int id]() returns AllTypes|persist:Error {
        AllTypes result = check self->/alltypes/[id];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ALL_TYPES);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get stringidrecords(StringIdRecordTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get stringidrecords/[string id](StringIdRecordTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post stringidrecords(StringIdRecordInsert[] data) returns string[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(STRING_ID_RECORD);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from StringIdRecordInsert inserted in data
            select inserted.id;
    }

    isolated resource function put stringidrecords/[string id](StringIdRecordUpdate value)
    returns StringIdRecord|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(STRING_ID_RECORD);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/stringidrecords/[id];
    }

    isolated resource function delete stringidrecords/[string id]() returns StringIdRecord|persist:Error {
        StringIdRecord result = check self->/stringidrecords/[id];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(STRING_ID_RECORD);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get intidrecords(IntIdRecordTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get intidrecords/[int id](IntIdRecordTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post intidrecords(IntIdRecordInsert[] data) returns int[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(INT_ID_RECORD);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from IntIdRecordInsert inserted in data
            select inserted.id;
    }

    isolated resource function put intidrecords/[int id](IntIdRecordUpdate value) returns IntIdRecord|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(INT_ID_RECORD);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/intidrecords/[id];
    }

    isolated resource function delete intidrecords/[int id]() returns IntIdRecord|persist:Error {
        IntIdRecord result = check self->/intidrecords/[id];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(INT_ID_RECORD);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get floatidrecords(FloatIdRecordTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get floatidrecords/[float id](FloatIdRecordTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post floatidrecords(FloatIdRecordInsert[] data) returns float[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(FLOAT_ID_RECORD);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from FloatIdRecordInsert inserted in data
            select inserted.id;
    }

    isolated resource function put floatidrecords/[float id](FloatIdRecordUpdate value)
    returns FloatIdRecord|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(FLOAT_ID_RECORD);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/floatidrecords/[id];
    }

    isolated resource function delete floatidrecords/[float id]() returns FloatIdRecord|persist:Error {
        FloatIdRecord result = check self->/floatidrecords/[id];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(FLOAT_ID_RECORD);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get decimalidrecords(DecimalIdRecordTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get decimalidrecords/[decimal id](DecimalIdRecordTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post decimalidrecords(DecimalIdRecordInsert[] data) returns decimal[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(DECIMAL_ID_RECORD);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from DecimalIdRecordInsert inserted in data
            select inserted.id;
    }

    isolated resource function put decimalidrecords/[decimal id](DecimalIdRecordUpdate value)
    returns DecimalIdRecord|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(DECIMAL_ID_RECORD);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/decimalidrecords/[id];
    }

    isolated resource function delete decimalidrecords/[decimal id]() returns DecimalIdRecord|persist:Error {
        DecimalIdRecord result = check self->/decimalidrecords/[id];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(DECIMAL_ID_RECORD);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get booleanidrecords(BooleanIdRecordTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get booleanidrecords/[boolean id](BooleanIdRecordTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post booleanidrecords(BooleanIdRecordInsert[] data) returns boolean[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(BOOLEAN_ID_RECORD);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from BooleanIdRecordInsert inserted in data
            select inserted.id;
    }

    isolated resource function put booleanidrecords/[boolean id](BooleanIdRecordUpdate value)
    returns BooleanIdRecord|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(BOOLEAN_ID_RECORD);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/booleanidrecords/[id];
    }

    isolated resource function delete booleanidrecords/[boolean id]() returns BooleanIdRecord|persist:Error {
        BooleanIdRecord result = check self->/booleanidrecords/[id];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(BOOLEAN_ID_RECORD);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get compositeassociationrecords(CompositeAssociationRecordTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get
    compositeassociationrecords/[string id](CompositeAssociationRecordTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post compositeassociationrecords(CompositeAssociationRecordInsert[] data)
    returns string[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(COMPOSITE_ASSOCIATION_RECORD);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from CompositeAssociationRecordInsert inserted in data
            select inserted.id;
    }

    isolated resource function put compositeassociationrecords/[string id](CompositeAssociationRecordUpdate value)
    returns CompositeAssociationRecord|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(COMPOSITE_ASSOCIATION_RECORD);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/compositeassociationrecords/[id];
    }

    isolated resource function delete compositeassociationrecords/[string id]()
    returns CompositeAssociationRecord|persist:Error {
        CompositeAssociationRecord result = check self->/compositeassociationrecords/[id];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(COMPOSITE_ASSOCIATION_RECORD);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get alltypesidrecords(AllTypesIdRecordTargetType targetType = <>)
    returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get alltypesidrecords/[boolean booleanType]/[int intType]/[float floatType]
    /[decimal decimalType]/[string stringType](AllTypesIdRecordTargetType targetType = <>)
    returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post alltypesidrecords(AllTypesIdRecordInsert[] data) returns [boolean, int, float,
    decimal, string][]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ALL_TYPES_ID_RECORD);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from AllTypesIdRecordInsert inserted in data
            select [
                inserted.booleanType,
                inserted.intType,
                inserted.floatType,
                inserted.decimalType,
                inserted.stringType
            ];
    }

    isolated resource function put alltypesidrecords/[boolean booleanType]/[int intType]/[float floatType]
    /[decimal decimalType]/[string stringType](AllTypesIdRecordUpdate value) returns AllTypesIdRecord|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ALL_TYPES_ID_RECORD);
        }
        _ = check redisClient.runUpdateQuery({
            "booleanType": booleanType,
            "intType": intType,
            "floatType":
            floatType,
            "decimalType": decimalType,
            "stringType": stringType
        }, value);
        return self->/alltypesidrecords/[booleanType]/[intType]/[floatType]/[decimalType]/[stringType];
    }

    isolated resource function delete
    alltypesidrecords/[boolean booleanType]/[int intType]/[float floatType]/[decimal decimalType]/[string stringType]()
    returns AllTypesIdRecord|persist:Error {
        AllTypesIdRecord result =
        check self->/alltypesidrecords/[booleanType]/[intType]/[floatType]/[decimalType]/[stringType];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ALL_TYPES_ID_RECORD);
        }
        _ = check redisClient.runDeleteQuery({
            "booleanType": booleanType,
            "intType": intType,
            "floatType":
            floatType,
            "decimalType": decimalType,
            "stringType": stringType
        });
        return result;
    }

    public isolated function close() returns persist:Error? {
        error? result = self.dbClient.close();
        if result is error {
            return <persist:Error>error(result.message());
        }
        return result;
    }
}
