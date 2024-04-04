// Copyright (c) 2024 WSO2 LLC. (http://www.wso2.com).
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
// AUTO-GENERATED FILE. DO NOT MODIFY.
// This file is an auto-generated file by Ballerina persistence layer for model.
// It should not be modified by hand.
import ballerina/jballerina.java;
import ballerina/persist;
import ballerinax/redis;

final redis:ConnectionConfig & readonly redisCache = {connection: "redis://localhost:6378"};

const PERSON = "people";
const APARTMENT = "apartments";

public isolated client class RedisCacheClient {
    *persist:AbstractPersistClient;

    private final redis:Client dbClient;

    private final map<RedisClient> persistClients;

    private final record {|RedisMetadata...;|} & readonly metadata = {
        [PERSON]: {
            entityName: "Person",
            collectionName: "Person",
            fieldMetadata: {
                id: {fieldName: "id", fieldDataType: INT},
                name: {fieldName: "name", fieldDataType: STRING},
                "soldBuildings[].code": {relation: {entityName: "soldBuildings", refField: "code", refFieldDataType: STRING}},
                "soldBuildings[].city": {relation: {entityName: "soldBuildings", refField: "city", refFieldDataType: STRING}},
                "soldBuildings[].state": {relation: {entityName: "soldBuildings", refField: "state", refFieldDataType: STRING}},
                "soldBuildings[].country": {relation: {entityName: "soldBuildings", refField: "country", refFieldDataType: STRING}},
                "soldBuildings[].postalCode": {relation: {entityName: "soldBuildings", refField: "postalCode", refFieldDataType: STRING}},
                "soldBuildings[].type": {relation: {entityName: "soldBuildings", refField: "type", refFieldDataType: STRING}},
                "soldBuildings[].soldpersonId": {relation: {entityName: "soldBuildings", refField: "soldpersonId", refFieldDataType: INT}},
                "soldBuildings[].ownpersonId": {relation: {entityName: "soldBuildings", refField: "ownpersonId", refFieldDataType: INT}},
                "ownBuildings[].code": {relation: {entityName: "ownBuildings", refField: "code", refFieldDataType: STRING}},
                "ownBuildings[].city": {relation: {entityName: "ownBuildings", refField: "city", refFieldDataType: STRING}},
                "ownBuildings[].state": {relation: {entityName: "ownBuildings", refField: "state", refFieldDataType: STRING}},
                "ownBuildings[].country": {relation: {entityName: "ownBuildings", refField: "country", refFieldDataType: STRING}},
                "ownBuildings[].postalCode": {relation: {entityName: "ownBuildings", refField: "postalCode", refFieldDataType: STRING}},
                "ownBuildings[].type": {relation: {entityName: "ownBuildings", refField: "type", refFieldDataType: STRING}},
                "ownBuildings[].soldpersonId": {relation: {entityName: "ownBuildings", refField: "soldpersonId", refFieldDataType: INT}},
                "ownBuildings[].ownpersonId": {relation: {entityName: "ownBuildings", refField: "ownpersonId", refFieldDataType: INT}}
            },
            keyFields: ["id"],
            refMetadata: {
                soldBuildings: {entity: Apartment, fieldName: "soldBuildings", refCollection: "Apartment", refFields: ["soldpersonId"], joinFields: ["id"], 'type: MANY_TO_ONE},
                ownBuildings: {entity: Apartment, fieldName: "ownBuildings", refCollection: "Apartment", refFields: ["ownpersonId"], joinFields: ["id"], 'type: MANY_TO_ONE}
            }
        },
        [APARTMENT]: {
            entityName: "Apartment",
            collectionName: "Apartment",
            fieldMetadata: {
                code: {fieldName: "code", fieldDataType: STRING},
                city: {fieldName: "city", fieldDataType: STRING},
                state: {fieldName: "state", fieldDataType: STRING},
                country: {fieldName: "country", fieldDataType: STRING},
                postalCode: {fieldName: "postalCode", fieldDataType: STRING},
                'type: {fieldName: "type", fieldDataType: STRING},
                soldpersonId: {fieldName: "soldpersonId", fieldDataType: INT},
                ownpersonId: {fieldName: "ownpersonId", fieldDataType: INT},
                "soldPerson.id": {relation: {entityName: "soldPerson", refField: "id", refFieldDataType: INT}},
                "soldPerson.name": {relation: {entityName: "soldPerson", refField: "name", refFieldDataType: STRING}},
                "ownPerson.id": {relation: {entityName: "ownPerson", refField: "id", refFieldDataType: INT}},
                "ownPerson.name": {relation: {entityName: "ownPerson", refField: "name", refFieldDataType: STRING}}
            },
            keyFields: ["code"],
            refMetadata: {
                soldPerson: {entity: Person, fieldName: "soldPerson", refCollection: "Person", refMetaDataKey: "soldBuildings", refFields: ["id"], joinFields: ["soldpersonId"], 'type: ONE_TO_MANY},
                ownPerson: {entity: Person, fieldName: "ownPerson", refCollection: "Person", refMetaDataKey: "ownBuildings", refFields: ["id"], joinFields: ["ownpersonId"], 'type: ONE_TO_MANY}
            }
        }
    };

    public isolated function init() returns persist:Error? {
        redis:Client|error dbClient = new (connectionConfig);
        if dbClient is error {
            return <persist:Error>error(dbClient.message());
        }
        self.dbClient = dbClient;
        self.persistClients = {
            [PERSON]: check new (dbClient, self.metadata.get(PERSON), 4),
            [APARTMENT]: check new (dbClient, self.metadata.get(APARTMENT), 4)
        };
    }

    isolated resource function get people(PersonTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get people/[int id](PersonTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post people(PersonInsert[] data) returns int[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(PERSON);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from PersonInsert inserted in data
            select inserted.id;
    }

    isolated resource function put people/[int id](PersonUpdate value) returns Person|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(PERSON);
        }
        _ = check redisClient.runUpdateQuery(id, value);
        return self->/people/[id].get();
    }

    isolated resource function delete people/[int id]() returns Person|persist:Error {
        Person result = check self->/people/[id].get();
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(PERSON);
        }
        _ = check redisClient.runDeleteQuery(id);
        return result;
    }

    isolated resource function get apartments(ApartmentTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get apartments/[string code](ApartmentTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post apartments(ApartmentInsert[] data) returns string[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(APARTMENT);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from ApartmentInsert inserted in data
            select inserted.code;
    }

    isolated resource function put apartments/[string code](ApartmentUpdate value) returns Apartment|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(APARTMENT);
        }
        _ = check redisClient.runUpdateQuery(code, value);
        return self->/apartments/[code].get();
    }

    isolated resource function delete apartments/[string code]() returns Apartment|persist:Error {
        Apartment result = check self->/apartments/[code].get();
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(APARTMENT);
        }
        _ = check redisClient.runDeleteQuery(code);
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
