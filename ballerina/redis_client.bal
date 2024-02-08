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

import ballerina/persist;
import ballerinax/redis;
import ballerina/io;
import ballerina/time;

# The client used by the generated persist clients to abstract and 
# execute Redis database operations that are required to perform CRUD operations.
public isolated client class RedisClient {

    private final redis:Client dbClient;

    private final string entityName;
    private final string collectionName;
    private final map<FieldMetadata> & readonly fieldMetadata;
    private final string[] & readonly keyFields;
    private final map<RefMetadata> & readonly refMetadata;

    # Initializes the `RedisClient`.
    #
    # + dbClient - The `redis:Client`, which is used to execute Redis database operations.
    # + metadata - Metadata of the entity
    # + return - A `persist:Error` if the client creation fails
    public isolated function init(redis:Client dbClient, RedisMetadata & readonly metadata) returns persist:Error? {
        self.entityName = metadata.entityName;
        self.collectionName = metadata.collectionName;
        self.fieldMetadata = metadata.fieldMetadata;
        self.keyFields = metadata.keyFields;
        self.dbClient = dbClient;
        (map<RefMetadata> & readonly)? refMetadata = metadata.refMetadata;
        if refMetadata is map<RefMetadata> & readonly {
            self.refMetadata = refMetadata;
        } else {
            self.refMetadata = {};
        }
    }

    # Performs a batch `HGET` operation to get entity instances as a stream
    #
    # + rowType - The type description of the entity to be retrieved
    # + typeMap - The data type map of the target type
    # + key - Key for the record
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + typeDescriptions - The type descriptions of the relations to be retrieved
    # + return - An `record {|anydata...;|}` containing the requested record
    # or a `persist:Error` if the operation fails
    public isolated function runReadByKeyQuery(typedesc<record {}> rowType, map<anydata> typeMap, anydata key, 
    string[] fields = [], string[] include = [], typedesc<record {}>[] typeDescriptions = []) 
    returns record {|anydata...;|}|persist:Error {
        // Generate the key
        string recordKey = string `${self.collectionName}${self.getKey(key)}`;
        do {
            // Handling simple fields
            record {} 'object = check self.querySimpleFieldsByKey(typeMap, recordKey, fields);
            // Handling relation fields
            check self.getManyRelations(typeMap, 'object, fields, include);
            self.removeUnwantedFields('object, fields);
            self.removeNonExistOptionalFields('object);
            return check 'object.cloneWithType(rowType);
        } on fail error e {
            return error persist:Error(e.message());
        }
    }

    # Performs a batch `HGET` operation to get entity instances as a stream
    #
    # + rowType - The type description of the entity to be retrieved
    # + typeMap - The data types of the record
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of `stream<record{}|error?>` containing the requested records
    # or a `persist:Error` if the operation fails
    public isolated function runReadQuery(typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [], string[] include = []) returns stream<record {}|error?>|persist:Error {
        // Get all the keys
        string[]|error keys = self.dbClient->keys(string `${self.collectionName}${KEY_SEPERATOR}*`);
        if keys is error {
            return error persist:Error(keys.message());
        }

        // Get data one by one using the key
        record {}[] result = [];
        foreach string key in keys {
            // Verifying the key belongs to a hash
            string redisType = check self.dbClient->redisType(key);
            if redisType != REDIS_HASH {
                continue;
            }

            // Handling simple fields only for batch read
            record {} 'object = check self.querySimpleFieldsByKey(typeMap, key, fields);
            // check self.getManyRelations(typeMap, 'object, fields, include);
            self.removeUnwantedFields('object, fields);
            self.removeNonExistOptionalFields('object);
            result.push('object);
        } on fail error e {
            return error persist:Error(e.message());
        }
        return stream from record {} rec in result
            select rec;
    }

    # Performs a batch `HMSET` operation to insert entity instances into a collection.
    #
    # + insertRecords - The entity records to be inserted into the collection
    # + return - A `string` containing the information of the database operation execution
    # or a `persist:Error` if the operation fails
    public isolated function runBatchInsertQuery(record {}[] insertRecords) returns string|persist:Error {
        
        string|error result;
        // For each record, do HMSET
        foreach var insertRecord in insertRecords {

            // Convert tye 'time:Date' and 'time:TimeOfDay' data types to string
            record {} newInsertRecord = check self.getRecordWithDateTime(insertRecord);

            // Generate the key
            string key = "";
            foreach string keyField in self.keyFields {
                key += string `${KEY_SEPERATOR}${insertRecord[keyField].toString()}`;
            }

            // Check for duplicate keys withing the collection
            int isKeyExists = check self.dbClient->exists([self.collectionName + key]);
            if isKeyExists != 0 {
                return persist:getAlreadyExistsError(self.collectionName, self.collectionName + key);
            }

            // Check for any relation field constraints
            check self.checkRelationFieldConstraints(key, insertRecord);

            // Insert the object
            result = self.dbClient->hMSet(self.collectionName + key, newInsertRecord);
            if result is error {
                return error persist:Error(result.message());
            }
        } on fail error e {
            return error persist:Error(e.message());
        }

        // Decide how to log queries

        if result is string {
            return result;
        }
        return error persist:Error(result.message());
    }

    # Performs redis `DEL` operation to delete an entity record from the database.
    #
    # + key - The ordered keys used to delete an entity record
    # + return - `()` if the operation is performed successfully 
    # or a `persist:Error` if the operation fails
    public isolated function runDeleteQuery(anydata key) returns persist:Error? {
        // Generate the key
        string recordKey = string `${self.collectionName}${self.getKey(key)}`;

        do {
            // Delete the record
            _ = check self.dbClient->del([recordKey]);
        } on fail error e {
            return error persist:Error(e.message());
        }
    }

    # Performs redis `HSET` operation to delete an entity record from the database.
    #
    # + key - The ordered keys used to update an entity record
    # + updateRecord - The new record to be updated
    # + return - An Error if the new record is missing a keyfield
    public isolated function runUpdateQuery(anydata key, record {} updateRecord) returns error? {
        // Generate the key
        string recordKey = string `${self.collectionName}${self.getKey(key)}`;

        // Update only the given fields that is not nil
        foreach [string, FieldMetadata & readonly] metaDataEntry in self.fieldMetadata.entries() {
            FieldMetadata & readonly fieldMetadataValue = metaDataEntry[1];

            // If the field is a simple field
            if fieldMetadataValue is SimpleFieldMetadata {
                if updateRecord.hasKey(fieldMetadataValue.fieldName) 
                && updateRecord[fieldMetadataValue.fieldName] != () {
                    // updating the object
                    _ = check self.dbClient->hSet(recordKey, fieldMetadataValue.fieldName, 
                    updateRecord[fieldMetadataValue.fieldName].toString());
                }
            } else {
                // If the field is a relation field
            }
        }
    }

    # Retrieves all the associations of a given object
    #
    # + typeMap - The data types of the record
    # + object - The object of the interest
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A `persist:Error` if the operation fails
    public isolated function getManyRelations(map<anydata> typeMap, record {} 'object, string[] fields, 
    string[] include) returns persist:Error? {

        foreach int i in 0 ..< include.length() {
            string entity = include[i];
            CardinalityType cardinalityType = ONE_TO_MANY;

            // checking for one to many relationships
            string[] relationFields = from string 'field in fields
                where 'field.startsWith(string `${entity}${MANY_ASSOCIATION_SEPERATOR}`)
                select 'field.substring(entity.length() + 3, 'field.length());

            // checking for one to one relationships
            if relationFields.length() == 0 {
                relationFields = from string 'field in fields
                    where 'field.startsWith(string `${entity}${ASSOCIATION_SEPERATOR}`)
                    select 'field.substring(entity.length() + 1, 'field.length());

                if relationFields.length() != 0 {
                    cardinalityType = ONE_TO_ONE;
                }
            }

            if relationFields.length() == 0 {
                continue;
            }

            string[]|error keys = self.dbClient->keys(
                string `${entity.substring(0, 1).toUpperAscii()}${entity.substring(1)}${KEY_SEPERATOR}*`);
            if keys is error || keys.length() == 0 {
                if cardinalityType == ONE_TO_MANY {
                    'object[entity] = [];
                } else {
                    'object[entity] = {};
                }
                continue;
            }

            // Get data one by one using the key
            record {}[] associatedRecords = [];
            foreach string key in keys {
                // Handling simple fields
                record {} valueToRecord = check self.queryRelationFieldsByKey(entity, cardinalityType, key, relationFields);

                // Check whether the record is associated with the current object
                boolean isAssociated = true;
                foreach string keyField in self.keyFields {
                    string refField = self.entityName.substring(0, 1).toLowerAscii() + self.entityName.substring(1)
                    + keyField.substring(0, 1).toUpperAscii() + keyField.substring(1);
                    boolean isSimilar = valueToRecord[refField] == 'object[keyField];
                    if !isSimilar {
                        isAssociated = false;
                    }
                }

                if isAssociated {
                    foreach string refField in valueToRecord.keys() {
                        if relationFields.indexOf(refField) is () {
                            _ = valueToRecord.remove(refField);
                        }
                    }
                    associatedRecords.push(valueToRecord);
                }
            }

            if associatedRecords.length() > 0 {
                if cardinalityType == ONE_TO_ONE {
                    'object[entity] = associatedRecords[0];
                } else {
                    'object[entity] = associatedRecords;
                }
            }
        } on fail persist:Error e {
            return e;
        }
    }

    public isolated function getKeyFields() returns string[] {
        return self.keyFields;
    }

    // Private helper methods
    private isolated function getKey(anydata key) returns string {
        string keyValue = "";
        if key is map<anydata> {
            foreach string compositeKey in self.keyFields {
                keyValue += string `${KEY_SEPERATOR}${key[compositeKey].toString()}`;
            }
            return keyValue;
        }
        return string `${KEY_SEPERATOR}${key.toString()}`;
    }

    private isolated function querySimpleFieldsByKey(map<anydata> typeMap, string key, string[] fields) 
    returns record {|anydata...;|}|error {
        // hadling the simple fields
        string[] simpleFields = self.getTargetSimpleFields(fields, typeMap);
        // If no simpleFields given, then add all the fields by default
        if simpleFields == [] {
            foreach [string, FieldMetadata & readonly] metaDataEntry in self.fieldMetadata.entries() {
                FieldMetadata & readonly fieldMetadataValue = metaDataEntry[1];

                // If the field is a simple field
                if fieldMetadataValue is SimpleFieldMetadata {
                    simpleFields.push(fieldMetadataValue.fieldName);
                }
            }
        }

        do {
            // Retrieve the record
            map<any> value = check self.dbClient->hMGet(key, simpleFields);
            if self.isNoRecordFound(value) {
                return persist:getNotFoundError(self.entityName, key);
            }
            record {} valueToRecord = {};
            foreach string fieldKey in value.keys() {
                // convert the data type from 'any' to required type
                valueToRecord[fieldKey] = check self.dataConverter(
                    <FieldMetadata & readonly>self.fieldMetadata[fieldKey], <anydata>value[fieldKey]);
            }
            return valueToRecord;
        } on fail error e {
            return error persist:Error(e.message());
        }
    }

    private isolated function queryRelationFieldsByKey(string entity, CardinalityType cardinalityType, string key, string[] fields) returns record {|anydata...;|}|persist:Error {
        // If the field doesn't containes reference fields, add them here
        string[] relationFields = fields.clone();
        foreach string keyField in self.keyFields {
            string refField = self.entityName.substring(0, 1).toLowerAscii() + self.entityName.substring(1)
            + keyField.substring(0, 1).toUpperAscii() + keyField.substring(1);
            if relationFields.indexOf(refField) is () {
                relationFields.push(refField);
            }
        }

        do {
            // Retrieve related records
            map<any> value = check self.dbClient->hMGet(key, relationFields);
            if self.isNoRecordFound(value) {
                return error persist:Error(string `No '${self.entityName}' found for the given key`);
            }

            record {} valueToRecord = {};
            string fieldMetadataKeyPrefix = entity;
            if cardinalityType == ONE_TO_MANY {
                fieldMetadataKeyPrefix += MANY_ASSOCIATION_SEPERATOR;
            } else {
                fieldMetadataKeyPrefix += ASSOCIATION_SEPERATOR;
            }

            foreach string fieldKey in value.keys() {
                // convert the data type from 'any' to required type
                valueToRecord[fieldKey] = check self.dataConverter(
                    <FieldMetadata & readonly>self.fieldMetadata[fieldMetadataKeyPrefix + fieldKey], <anydata>value[fieldKey]);
            }
            return valueToRecord;
        } on fail error e {
            return error persist:Error(e.message());
        }
    }

    private isolated function getTargetSimpleFields(string[] fields, map<anydata> typeMap) returns string[] {
        return from string 'field in fields
            where !'field.includes(".") && typeMap.hasKey('field)
            select 'field;
    }

    private isolated function removeNonExistOptionalFields(record {} 'object) {
        foreach string key in 'object.keys() {
            if 'object[key] == () {
                _ = 'object.remove(key);
            }
        }
    }

    private isolated function isNoRecordFound(map<any> value) returns boolean {
        foreach string key in value.keys() {
            if value[key] != () {
                return false;
            }
        }
        return true;
    }

    private isolated function removeUnwantedFields(record {} 'object, string[] fields) {
        string[] keyFields = self.keyFields;
        foreach string keyField in keyFields {
            if fields.indexOf(keyField) is () {
                _ = 'object.remove(keyField);
            }
        }
    }

    private isolated function checkRelationFieldConstraints(string key, record {} insertRecord) returns persist:Error? {
        if self.refMetadata != {} {
            foreach RefMetadata & readonly refMetadataValue in self.refMetadata {
                //  If the entity is not the relation owner
                if refMetadataValue.joinFields == self.keyFields {
                    continue;
                }

                boolean isRelationConstraintFalied = true;
                // If the mandatory reference field is not exist or being null
                foreach string joinField in refMetadataValue.joinFields {
                    if insertRecord.hasKey(joinField) && insertRecord[joinField] != () {
                        isRelationConstraintFalied = false;
                        break;
                    }
                }

                if !isRelationConstraintFalied {
                    // Generate the key to reference record
                    string refRecordKey = refMetadataValue.refCollection;
                    foreach string joinField in refMetadataValue.joinFields {
                        refRecordKey += string `${KEY_SEPERATOR}${insertRecord[joinField].toString()}`;
                    }

                    // Check the cardinality of refered entity object
                    int|error sCard = self.dbClient->sCard(
                        string `${refRecordKey}${KEY_SEPERATOR}${self.collectionName}`);
                    if sCard is int && sCard > 0 && refMetadataValue.'type == ONE_TO_ONE {
                        // If the refered object is already in a relationship
                        return getConstraintViolationError(self.entityName, refMetadataValue.refCollection);
                    }

                    // Relate current record with the refered record in the database
                    int|error sAdd = self.dbClient->sAdd(
                        string `${refRecordKey}${KEY_SEPERATOR}${self.collectionName}`, [key]);
                    if sAdd is error {
                        return error persist:Error(sAdd.message());
                    }

                    map<any> value = check self.dbClient->hMGet(refRecordKey, refMetadataValue.refFields);
                    if self.isNoRecordFound(value) {
                        return getConstraintViolationError(self.entityName, refMetadataValue.refCollection);
                    }
                }
            } on fail error e {
                return error persist:Error(e.message());
            }
        }
    }

    private isolated function getRecordWithDateTime(record {} insertRecord) returns record {}|persist:Error {
        record {} newRecord = {};
        foreach string recordfield in insertRecord.keys() {
            (FieldMetadata & readonly)? fieldMetaDataValue = self.fieldMetadata[recordfield];
            if fieldMetaDataValue is SimpleFieldMetadata {
                DataType dataType = fieldMetaDataValue.fieldDataType;

                if dataType == DATE || dataType == TIME_OF_DAY {
                    RedisTimeType|error timeValue = insertRecord.get(recordfield).ensureType();
                    if timeValue is error {
                        return error persist:Error(timeValue.message());
                    }

                    string|persist:Error? timeValueInString = self.timeToString(timeValue);
                    if timeValueInString is persist:Error {
                        return timeValueInString;
                    }

                    io:println(typeof insertRecord[recordfield]);
                    newRecord[recordfield] = timeValueInString;
                }else{
                    newRecord[recordfield] = insertRecord[recordfield];
                }
            }
        }
        return newRecord;
    }

    private isolated function timeToString(RedisTimeType timeValue) returns string|persist:Error? {
        if timeValue is time:Date {
            return string `${timeValue.day}-${timeValue.month}-${timeValue.year}`;
        }

        if timeValue is time:TimeOfDay {
            return string `${timeValue.hour}-${timeValue.minute}-${(timeValue.second).toString()}`;
        }

        return error persist:Error("Error: unsupported time format");
    }

    private isolated function stringToTime(string timeValue, DataType dataType) returns RedisTimeType|error {
        if dataType == TIME_OF_DAY {
            string[] timeValues = re `-`.split(timeValue);
            time:TimeOfDay output = {hour: check int:fromString(timeValues[0]), minute: check int:fromString(timeValues[1]), second: check decimal:fromString(timeValues[2])};
            return output;
        } else if dataType == DATE {
            string[] timeValues = re `-`.split(timeValue);
            time:Date output = {day: check int:fromString(timeValues[0]), month: check int:fromString(timeValues[1]), year: check int:fromString(timeValues[2])};
            return output;
        } else {
            return error persist:Error("Error: unsupported time format");
        }
    }

    private isolated function dataConverter(FieldMetadata & readonly fieldMetaData, anydata value) returns ()|boolean|string|float|decimal|int|RedisTimeType|error {

        // Return nil if value is nil
        if value is () {
            return ();
        }

        if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == INT)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == INT) {
            return check int:fromString(<string>value);

        } else if (fieldMetaData is SimpleFieldMetadata && (fieldMetaData[FIELD_DATA_TYPE] == STRING))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == STRING) {
            return <string>value;

        } else if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == FLOAT)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == FLOAT) {
            return check float:fromString(<string>value);

        }else if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == DECIMAL)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == DECIMAL) {
            return check decimal:fromString(<string>value);

        }   else if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == BOOLEAN)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == BOOLEAN) {
            return check boolean:fromString(<string>value);

        } else if (fieldMetaData is SimpleFieldMetadata && (fieldMetaData[FIELD_DATA_TYPE] == ENUM))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == ENUM) {
            return <string>value;

        } else if (fieldMetaData is SimpleFieldMetadata && (fieldMetaData[FIELD_DATA_TYPE] == DATE))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == DATE) {
            return self.stringToTime(<string>value, DATE);

        } else if (fieldMetaData is SimpleFieldMetadata && (fieldMetaData[FIELD_DATA_TYPE] == TIME_OF_DAY))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == TIME_OF_DAY) {
            return self.stringToTime(<string>value, TIME_OF_DAY);

        } else {
            return error persist:Error("Unsupported Data Format");
        }
    }

}
