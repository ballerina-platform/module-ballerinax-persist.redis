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
import ballerina/lang.regexp;
import ballerina/log;
import ballerina/persist;
import ballerina/time;
import ballerinax/redis;

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
            return check 'object.cloneWithType(rowType);
        } on fail error e {
            if e is persist:NotFoundError || e is persist:Error {
                return e;
            }
            return error persist:Error(e.message(), e);
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
    public isolated function runReadQuery(typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [],
            string[] include = []) returns stream<record {}|error?>|persist:Error {
        // Get all the keys
        string pattern = string `${self.collectionName}${KEY_SEPERATOR}*`;
        string[]|error keys = self.dbClient->keys(pattern);
        if keys is error {
            return error persist:Error(keys.message(), keys);
        }
        self.logQuery(KEYS, pattern);

        // Get records one by one using the key
        record {}[] result = [];
        foreach string key in keys {
            // Validate the type
            string redisType = check self.dbClient->redisType(key);
            self.logQuery(REDISTYPE, key);
            if redisType != REDIS_HASH {
                continue;
            }

            // Handling simple fields
            record {} 'object = check self.querySimpleFieldsByKey(typeMap, key, fields);
            result.push('object);
        } on fail error e {
            if e is persist:NotFoundError || e is persist:Error {
                return e;
            }
            return error persist:Error(e.message(), e);
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

        // Verify key existance
        string[] insertKeys = [];
        foreach var insertRecord in insertRecords {
            // Generate the key
            string key = self.collectionName;
            foreach string keyField in self.keyFields {
                key += string `${KEY_SEPERATOR}${insertRecord[keyField].toString()}`;
            }
            insertKeys.push(key);
        }
        int|error isKeyExists = self.dbClient->exists(insertKeys);
        if isKeyExists is error {
            return error persist:Error(isKeyExists.message(), isKeyExists);
        }
        self.logQuery(EXISTS, insertKeys);
        if isKeyExists != 0 {
            return getAlreadyExistsError(self.collectionName, isKeyExists);
        }

        // For each record, do HMSET
        foreach var insertRecord in insertRecords {

            // Check time related data types
            boolean isContainTimeType = false;
            foreach string recordfield in insertRecord.keys() {
                (FieldMetadata & readonly)? fieldMetaDataValue = self.fieldMetadata[recordfield];
                if fieldMetaDataValue is SimpleFieldMetadata {
                    DataType dataType = fieldMetaDataValue.fieldDataType;
                    if dataType == DATE || dataType == TIME_OF_DAY || dataType == UTC || dataType == CIVIL {
                        isContainTimeType = true;
                        continue;
                    }
                }
            }

            // Generate the key
            string keySuffix = "";
            foreach string keyField in self.keyFields {
                keySuffix += string `${KEY_SEPERATOR}${insertRecord[keyField].toString()}`;
            }

            // Check for any relation field constraints
            persist:Error? checkConstraints = self.checkRelationFieldConstraints(keySuffix, insertRecord);
            if checkConstraints is persist:ConstraintViolationError {
                return checkConstraints;
            }
            string key = string `${self.collectionName}${keySuffix}`;

            // Insert the record
            if isContainTimeType {
                result = self.dbClient->hMSet(key, check self.newRecordWithDateTime(insertRecord));
            } else {
                result = self.dbClient->hMSet(key, insertRecord);
            }
            self.logQuery(HMSET, {key: key, "record": insertRecord.toJsonString()});
        } on fail persist:Error e {
            return e;
        }

        if result is string {
            return result;
        }
        return error persist:Error(result.message(), result);
    }

    # Performs redis `DEL` operation to delete an entity record from the database.
    #
    # + key - The ordered keys used to delete an entity record
    # + return - `()` if the operation is performed successfully 
    # or a `persist:Error` if the operation fails
    public isolated function runDeleteQuery(anydata key) returns persist:Error? {
        string keySuffix = string `${self.getKey(key)}`;
        string keyWithPrefix = string `${self.collectionName}${keySuffix}`;
        // Delete the record
        do {
            // Check for references
            string[] allRefFields = [];
            foreach RefMetadata refMedaData in self.refMetadata {
                allRefFields.push(...refMedaData.joinFields);
            }

            // Remove any references if exists
            if allRefFields.length() > 0 {
                map<any> currentObject = check self.dbClient->hMGet(keyWithPrefix, allRefFields);
                self.logQuery(HMGET, {key: keyWithPrefix, fieldNames: allRefFields.toJsonString()});
                foreach RefMetadata refMetaData in self.refMetadata {
                    string refKey = refMetaData.refCollection;
                    foreach string refField in refMetaData.joinFields {
                        refKey += string `${KEY_SEPERATOR}${currentObject[refField].toString()}`;
                    }
                    string setKey = string `${refKey}${KEY_SEPERATOR}${refMetaData.refMetaDataKey ?: ""}`;
                    _ = check self.dbClient->sRem(setKey, [keySuffix.substring(1)]);
                    self.logQuery(SREM, {key: setKey, suffixes: [keySuffix].toJsonString()});
                }
            }

            // Remove the record
            _ = check self.dbClient->del([keyWithPrefix]);
            self.logQuery(DEL, [keyWithPrefix]);
        } on fail error e {
            return error persist:Error(e.message(), e);
        }
    }

    # Performs redis `HSET` operation to update an entity record from the database.
    #
    # + key - The ordered keys used to update an entity record
    # + updateRecord - The new record to be updated
    # + return - An Error if the new record is missing a keyfield
    public isolated function runUpdateQuery(anydata key, record {} updateRecord) returns persist:Error? {
        // Generate the key
        string recordKey = string `${self.collectionName}${self.getKey(key)}`;
        string recordKeySuffix = self.getKey(key);

        // Verify the existence of the key
        do {
            int isKeyExists = check self.dbClient->exists([recordKey]);
            self.logQuery(EXISTS, [recordKey]);
            if isKeyExists == 0 {
                return persist:getNotFoundError(self.collectionName, recordKey);
            }
        } on fail error e {
            if e is persist:NotFoundError {
                return e;
            }
            return error persist:Error(e.message(), e);
        }

        // Check time related data types
        boolean isContainTimeType = false;
        foreach string recordfield in updateRecord.keys() {
            (FieldMetadata & readonly)? fieldMetaDataValue = self.fieldMetadata[recordfield];
            if fieldMetaDataValue is SimpleFieldMetadata {
                DataType dataType = fieldMetaDataValue.fieldDataType;
                if dataType == DATE || dataType == TIME_OF_DAY || dataType == UTC || dataType == CIVIL {
                    isContainTimeType = true;
                    continue;
                }
            }
        }

        record {} newUpdateRecord = {};
        if isContainTimeType {
            newUpdateRecord = check self.newRecordWithDateTime(updateRecord);
        } else {
            newUpdateRecord = updateRecord;
        }

        // Get the original record before update
        map<()> updatedEntities = {};
        map<any>|error prevRecord = self.dbClient->hMGet(recordKey, newUpdateRecord.keys());
        if prevRecord is error {
            return error persist:Error(prevRecord.message(), prevRecord);
        }
        self.logQuery(HMGET, {key: recordKey, fieldNames: newUpdateRecord.keys().toJsonString()});

        // Check the validity of new associations
        foreach RefMetadata refMetaData in self.refMetadata {
            string[] joinFields = refMetaData.joinFields;

            // Recreate the key
            string relatedRecordKey = refMetaData.refCollection;
            foreach string joinField in joinFields {
                if newUpdateRecord.hasKey(joinField) {
                    updatedEntities[refMetaData.refCollection] = ();
                    relatedRecordKey += string `${KEY_SEPERATOR}${newUpdateRecord[joinField].toString()}`;
                } else {
                    relatedRecordKey += string `${KEY_SEPERATOR}${prevRecord[joinField].toString()}`;
                }
            }

            // Verify the new associated entities does exists
            if updatedEntities.hasKey(refMetaData.refCollection) {
                int isKeyExists = check self.dbClient->exists([relatedRecordKey]);
                self.logQuery(EXISTS, [relatedRecordKey]);
                if isKeyExists != 0 {
                    // Verify the key type as a HASH
                    string redisType = check self.dbClient->redisType(relatedRecordKey);
                    self.logQuery(REDISTYPE, relatedRecordKey);
                    if redisType == REDIS_HASH {
                        continue;
                    }
                }
                // Return a constrain violation error if new associations does not exists
                return getConstraintViolationError(self.collectionName, refMetaData.refCollection);
            }
        } on fail error e {
            if e is persist:ConstraintViolationError {
                return e;
            }
            return error persist:Error(e.message(), e);
        }

        // Verify the availablity of new associations
        // Eg: Reffered record might already in a ONE-TO-ONE relationship
        foreach RefMetadata refMetaData in self.refMetadata {
            if !updatedEntities.hasKey(refMetaData.refCollection) {
                continue;
            }
            string[] joinFields = refMetaData.joinFields;
            string newRelatedRecordKey = refMetaData.refCollection;
            foreach string joinField in joinFields {
                if newUpdateRecord.hasKey(joinField) {
                    newRelatedRecordKey += string `${KEY_SEPERATOR}${newUpdateRecord[joinField].toString()}`;
                } else {
                    newRelatedRecordKey += string `${KEY_SEPERATOR}${prevRecord[joinField].toString()}`;
                }
            }

            // Get keys of existing associations
            string setKey = string `${newRelatedRecordKey}${KEY_SEPERATOR}${refMetaData.refMetaDataKey ?: ""}`;
            int isKeyExists = check self.dbClient->exists([setKey]);
            self.logQuery(EXISTS, [setKey]);
            if isKeyExists != 0 {
                // Verify the key type as a SET
                string redisType = check self.dbClient->redisType(setKey);
                self.logQuery(REDISTYPE, setKey);
                if redisType == REDIS_SET {
                    // Check existing associations for ONE-TO-ONE
                    if refMetaData.'type == ONE_TO_ONE {
                        int cardinality = check self.dbClient->sCard(setKey);
                        self.logQuery(SCARD, setKey);
                        if cardinality > 0 {
                            return getConstraintViolationError(self.collectionName, refMetaData.refCollection);
                        }
                    }
                }
            }
        } on fail error e {
            if e is persist:ConstraintViolationError {
                return e;
            }
            return error persist:Error(e.message(), e);
        }

        // Update
        foreach string updatedField in newUpdateRecord.keys() {
            _ = check self.dbClient->hSet(recordKey, updatedField,
                    newUpdateRecord[updatedField].toString());
            self.logQuery(HSET, {
                key: recordKey,
                fieldName: updatedField,
                newValue: newUpdateRecord[updatedField].toJsonString()
            });
        } on fail error e {
            return error persist:Error(e.message(), e);
        }

        // Add new association to the SET
        foreach RefMetadata refMetaData in self.refMetadata {
            if !updatedEntities.hasKey(refMetaData.refCollection) {
                continue;
            }
            string[] joinFields = refMetaData.joinFields;
            string newRelatedRecordKey = refMetaData.refCollection;
            string prevRelatedRecordKey = refMetaData.refCollection;
            foreach string joinField in joinFields {
                if newUpdateRecord.hasKey(joinField) {
                    newRelatedRecordKey += string `${KEY_SEPERATOR}${newUpdateRecord[joinField].toString()}`;
                } else {
                    newRelatedRecordKey += string `${KEY_SEPERATOR}${prevRecord[joinField].toString()}`;
                }
                prevRelatedRecordKey += string `${KEY_SEPERATOR}${prevRecord[joinField].toString()}`;
            }
            // Attach to new association
            string newSetKey = string `${newRelatedRecordKey}${KEY_SEPERATOR}${refMetaData.refMetaDataKey ?: ""}`;
            int|error sAdd = self.dbClient->sAdd(newSetKey, [recordKeySuffix.substring(1)]);
            if sAdd is error {
                return error persist:Error(sAdd.message(), sAdd);
            }
            self.logQuery(SADD, {key: newSetKey, suffixes: [recordKeySuffix].toJsonString()});

            // Detach from previous association
            string prevSetKey = string `${prevRelatedRecordKey}${KEY_SEPERATOR}${refMetaData.refMetaDataKey ?: ""}`;
            int|error sRem = self.dbClient->sRem(prevSetKey, [recordKeySuffix.substring(1)]);
            if sRem is error {
                return error persist:Error(sRem.message(), sRem);
            }
            self.logQuery(SREM, {key: prevSetKey, suffixes: [recordKeySuffix].toJsonString()});
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
            (RefMetadata & readonly)? refMetaData = self.refMetadata[entity];
            if refMetaData == () {
                continue;
            }

            // Check for one to many relationships
            CardinalityType cardinalityType = ONE_TO_MANY;
            string[] relationFields = from string 'field in fields
                where 'field.startsWith(string `${entity}${MANY_ASSOCIATION_SEPERATOR}`)
                select 'field.substring(entity.length() + 3, 'field.length());

            // Check for one to one relationships
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

            // Get key suffixes of asslociated records
            string[]|error keySuffixes = self.getRelatedEntityKeySuffixes(entity, 'object);
            if keySuffixes is error || keySuffixes.length() == 0 {
                if cardinalityType == ONE_TO_MANY {
                    'object[entity] = [];
                } else {
                    'object[entity] = {};
                }
                continue;
            }

            // Get data one by one using the key
            record {}[] associatedRecords = [];
            foreach string key in keySuffixes {
                // Handling simple fields of the associated record
                record {} valueToRecord = check self.queryRelationFieldsByKey(entity, cardinalityType,
                string `${refMetaData.refCollection}${key}`, relationFields);

                foreach string refField in valueToRecord.keys() {
                    if relationFields.indexOf(refField) is () {
                        _ = valueToRecord.remove(refField);
                    }
                }
                associatedRecords.push(valueToRecord);
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

        self.removeUnwantedFields('object, fields);
        self.removeNonExistOptionalFields('object);
    }

    public isolated function getKeyFields() returns string[] {
        return self.keyFields;
    }

    // Private helper methods
    private isolated function getKey(anydata key) returns string {
        string keyValue = "";
        if key is map<any> {
            foreach string compositeKey in key.keys() {
                keyValue += string `${KEY_SEPERATOR}${key[compositeKey].toString()}`;
            }
            return keyValue;
        }
        return string `${KEY_SEPERATOR}${key.toString()}`;
    }

    private isolated function getKeyFromObject(record {} 'object) returns string {
        string key = self.collectionName;
        foreach string keyField in self.keyFields {
            key += string `${KEY_SEPERATOR}${'object[keyField].toString()}`;
        }
        return key;
    }

    private isolated function getRelatedEntityKeySuffixes(string entity, record {} 'object) returns string[]|error {
        (RefMetadata & readonly)? refMetaData = self.refMetadata[entity];
        if refMetaData == () {
            return [];
        }

        if refMetaData.joinFields == self.keyFields {
            // Non-owner have direct access to the association set
            string setKey = string `${self.getKeyFromObject('object)}${KEY_SEPERATOR}${refMetaData.fieldName}`;
            string[] keys = check self.dbClient->sMembers(setKey);
            self.logQuery(SMEMBERS, setKey);
            return from string key in keys
                select KEY_SEPERATOR + key;
        } else {
            map<any> recordWithRefFields = check self.dbClient->hMGet(self.getKeyFromObject('object),
            refMetaData.joinFields);
            self.logQuery(HMGET, {
                key: self.getKeyFromObject('object),
                "record": refMetaData.joinFields.toJsonString()
            });
            string key = "";
            foreach string joinField in refMetaData.joinFields {
                key += string `${KEY_SEPERATOR}${recordWithRefFields[joinField].toString()}`;
            }
            return [key];
        }
    }

    private isolated function querySimpleFieldsByKey(map<anydata> typeMap, string key, string[] fields)
    returns record {|anydata...;|}|persist:Error {
        string[] simpleFields = self.getTargetSimpleFields(fields, typeMap);

        do {
            // Retrieve the record
            map<any> value = check self.dbClient->hMGet(key, simpleFields);
            self.logQuery(HMGET, {key: key, fieldNames: simpleFields.toJsonString()});
            if self.isNoRecordFound(value) {
                return persist:getNotFoundError(self.entityName, key);
            }
            record {} valueToRecord = {};
            foreach string fieldKey in value.keys() {
                // Convert the data type from 'any' to relevent type
                valueToRecord[fieldKey] = check self.dataConverter(
                    <FieldMetadata & readonly>self.fieldMetadata[fieldKey], <anydata>value[fieldKey]);
            }
            return valueToRecord;
        } on fail error e {
            if e is persist:NotFoundError {
                return e;
            }
            return error persist:Error(e.message(), e);
        }
    }

    private isolated function queryRelationFieldsByKey(string entity, CardinalityType cardinalityType, string key,
            string[] fields) returns record {|anydata...;|}|persist:Error {
        string[] relationFields = fields.clone();
        (RefMetadata & readonly)? refMetaData = self.refMetadata[entity];
        if refMetaData == () {
            return error persist:Error(string `Undefined relation between ${self.entityName} and ${entity}`);
        }

        // Add required missing reference fields
        foreach string refKeyField in refMetaData.refFields {
            if relationFields.indexOf(refKeyField) is () {
                relationFields.push(refKeyField);
            }
        }

        do {
            // Retrieve related records
            map<any> value = check self.dbClient->hMGet(key, relationFields);
            self.logQuery(HMGET, {key: key, fieldNames: relationFields.toJsonString()});
            if self.isNoRecordFound(value) {
                return error persist:Error(string `No '${entity}' found for the given key '${key}'`);
            }

            record {} valueToRecord = {};
            string fieldMetadataKeyPrefix = entity;
            if cardinalityType == ONE_TO_MANY {
                fieldMetadataKeyPrefix += MANY_ASSOCIATION_SEPERATOR;
            } else {
                fieldMetadataKeyPrefix += ASSOCIATION_SEPERATOR;
            }

            foreach string fieldKey in value.keys() {
                // convert the data type from 'any' to relevant type
                valueToRecord[fieldKey] = check self.dataConverter(
                    <FieldMetadata & readonly>self.fieldMetadata[string `${fieldMetadataKeyPrefix}${fieldKey}`],
                    <anydata>value[fieldKey]);
            }
            return valueToRecord;
        } on fail error e {
            if e is persist:NotFoundError || e is persist:Error {
                return e;
            }
            return error persist:Error(e.message(), e);
        }
    }

    private isolated function getTargetSimpleFields(string[] fields, map<anydata> typeMap) returns string[] {
        string[] requiredFields = from string 'field in fields
            where !'field.includes(".") && typeMap.hasKey('field)
            select 'field;
        foreach string keyField in self.keyFields {
            if requiredFields.indexOf(keyField) == () {
                requiredFields.push(keyField);
            }
        }
        return requiredFields;
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
            if fields.indexOf(keyField) is () && 'object.hasKey(keyField) {
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

                // Generate the key to reference record
                string refRecordKey = refMetadataValue.refCollection;
                foreach string joinField in refMetadataValue.joinFields {
                    refRecordKey += string `${KEY_SEPERATOR}${insertRecord[joinField].toString()}`;
                }

                // Check the cardinality of refered entity record
                string setKey = string `${refRecordKey}${KEY_SEPERATOR}${refMetadataValue.refMetaDataKey ?: ""}`;
                int|error sCard = self.dbClient->sCard(setKey);
                self.logQuery(SCARD, setKey);
                if sCard is int && sCard > 0 && refMetadataValue.'type == ONE_TO_ONE {
                    // If the refered record is already in an association
                    return getConstraintViolationError(self.collectionName, refMetadataValue.refCollection);
                }

                // Associate current record with the refered record
                int|error sAdd = self.dbClient->sAdd(setKey, [key.substring(1)]);
                self.logQuery(SADD, {key: setKey, suffixes: [key].toJsonString()});
                if sAdd is error {
                    return error persist:Error(sAdd.message(), sAdd);
                }

                map<any> value = check self.dbClient->hMGet(refRecordKey, refMetadataValue.refFields);
                self.logQuery(HMGET, {key: refRecordKey, fieldNames: refMetadataValue.refFields.toJsonString()});
                if self.isNoRecordFound(value) {
                    return getConstraintViolationError(self.collectionName, refMetadataValue.refCollection);
                }
            } on fail error e {
                if e is persist:ConstraintViolationError {
                    return e;
                }
                return error persist:Error(e.message(), e);
            }
        }
    }

    private isolated function newRecordWithDateTime(record {} insertRecord) returns record {}|persist:Error {
        record {} newRecord = {};
        foreach string recordfield in insertRecord.keys() {
            (FieldMetadata & readonly)? fieldMetaDataValue = self.fieldMetadata[recordfield];
            if fieldMetaDataValue is SimpleFieldMetadata {
                DataType dataType = fieldMetaDataValue.fieldDataType;

                if dataType == DATE || dataType == TIME_OF_DAY || dataType == UTC || dataType == CIVIL {
                    RedisTimeType?|error timeValue = insertRecord.get(recordfield).ensureType();
                    if timeValue is error {
                        return error persist:Error(timeValue.message(), timeValue);
                    }

                    string|persist:Error? timeValueInString = self.timeToString(timeValue);
                    if timeValueInString is persist:Error {
                        return timeValueInString;
                    }

                    newRecord[recordfield] = timeValueInString;
                } else {
                    newRecord[recordfield] = insertRecord[recordfield];
                }
            }
        }
        return newRecord;
    }

    private isolated function timeToString(RedisTimeType? timeValue) returns string|persist:Error? {
        if timeValue is () {
            return ();
        }

        if timeValue is time:Civil {
            string|error civilToStringResult = self.civilToString(timeValue);
            if civilToStringResult is error {
                return error persist:Error(civilToStringResult.message(), civilToStringResult);
            }
            return civilToStringResult;
        }

        if timeValue is time:Utc {
            return time:utcToString(timeValue);
        }

        if timeValue is time:Date {
            return string `${timeValue.day}-${timeValue.month}-${timeValue.year}`;
        }

        if timeValue is time:TimeOfDay {
            return string `${timeValue.hour}:${timeValue.minute}:${(timeValue.second).toString()}`;
        }

        return error persist:Error("Error: unsupported time format");
    }

    private isolated function stringToTime(string timeValue, DataType dataType) returns RedisTimeType|error {
        if dataType == TIME_OF_DAY {
            string[] timeValues = re `:`.split(timeValue);
            time:TimeOfDay output = {
                hour: check int:fromString(timeValues[0]),
                minute: check int:fromString(
                        timeValues[1]),
                second: check decimal:fromString(timeValues[2])
            };
            return output;
        } else if dataType == DATE {
            string[] timeValues = re `-`.split(timeValue);
            time:Date output = {
                day: check int:fromString(timeValues[0]),
                month: check int:fromString(timeValues[1]),
                year: check int:fromString(timeValues[2])
            };
            return output;
        } else if dataType == CIVIL {
            return self.civilFromString(timeValue);
        } else if dataType == UTC {
            return time:utcFromString(timeValue);
        } else {
            return error persist:Error("Error: unsupported time format");
        }
    }

    private isolated function civilToString(time:Civil civil) returns string|error {
        string civilString = string `${civil.year}-${(civil.month.abs() > 9 ? civil.month
            : string `0${civil.month}`)}-${(civil.day.abs() > 9 ? civil.day : string `0${civil.day}`)}`;
        civilString += string `T${(civil.hour.abs() > 9 ? civil.hour
            : string `0${civil.hour}`)}:${(civil.minute.abs() > 9 ? civil.minute : string `0${civil.minute}`)}`;
        if civil.second !is () {
            time:Seconds seconds = <time:Seconds>civil.second;
            civilString += string `:${(seconds.abs() > (check decimal:fromString("9")) ? seconds
                : string `0${seconds}`)}`;
        }
        if civil.utcOffset !is () {
            time:ZoneOffset zoneOffset = <time:ZoneOffset>civil.utcOffset;
            civilString += (zoneOffset.hours >= 0 ? "+" : "-");
            civilString += string `${zoneOffset.hours.abs() > 9 ? zoneOffset.hours.abs()
                : string `0${zoneOffset.hours.abs()}`}`;
            civilString += string `:${(zoneOffset.minutes.abs() > 9 ? zoneOffset.minutes.abs()
                : string `0${zoneOffset.minutes.abs()}`)}`;
            time:Seconds? seconds = zoneOffset.seconds;
            if seconds !is () {
                civilString += string `:${(seconds.abs() > 9d ? seconds : string `0${seconds.abs()}`)}`;
            } else {
                civilString += string `:00`;
            }
        }
        if civil.timeAbbrev !is () {
            civilString += string `(${<string>civil.timeAbbrev})`;
        }
        return civilString;
    }

    private isolated function civilFromString(string civilString) returns time:Civil|error {
        time:ZoneOffset? zoneOffset = ();
        string civilTimeString = "";
        string civilDateString = "";
        string? timeAbbrev = ();
        regexp:Span? find = re `\(.*\)`.find(civilString.trim(), 0);
        if find !is () {
            timeAbbrev = civilString.trim().substring(find.startIndex + 1, find.endIndex - 1);
        }
        string[] civilArray = re `T`.split(re `\(.*\)`.replace(civilString.trim(), ""));
        civilDateString = civilArray[0];
        find = re `\+|-`.find(civilArray[1], 0);
        if find !is () {
            int sign = +1;
            if civilArray[1].includes("-") {
                sign = -1;
            }
            string[] civilTimeOffsetArray = re `\+|-`.split(civilArray[1]);
            civilTimeString = civilTimeOffsetArray[0];
            string[] zoneOffsetStringArray = re `:`.split(civilTimeOffsetArray[1]);
            zoneOffset = {
                hours: sign * (check int:fromString(zoneOffsetStringArray[0])),
                minutes: sign * (check int:fromString(zoneOffsetStringArray[1])),
                seconds: sign * (check decimal:fromString(zoneOffsetStringArray[2]))
            };
        } else {
            civilTimeString = civilArray[1];
        }
        string[] civilTimeStringArray = re `:`.split(civilTimeString);
        string[] civilDateStringArray = re `-`.split(civilDateString);
        int year = check int:fromString(civilDateStringArray[0]);
        int month = check int:fromString(civilDateStringArray[1]);
        int day = check int:fromString(civilDateStringArray[2]);
        int hour = check int:fromString(civilTimeStringArray[0]);
        int minute = check int:fromString(civilTimeStringArray[1]);
        decimal second = check decimal:fromString(civilTimeStringArray[2]);
        return <time:Civil>{
            year: year,
            month: month,
            day: day,
            hour: hour,
            minute: minute,
            second: second,
            timeAbbrev: timeAbbrev,
            utcOffset: zoneOffset
        };
    }

    private isolated function dataConverter(FieldMetadata & readonly fieldMetaData, anydata value)
    returns ()|boolean|string|float|decimal|int|RedisTimeType|error {

        // Return nil if value is nil
        if value is () {
            return ();
        }

        if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == INT)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == INT) {
            return check int:fromString(value.toString());

        } else if (fieldMetaData is SimpleFieldMetadata && (fieldMetaData[FIELD_DATA_TYPE] == STRING))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == STRING) {
            return <string>value;

        } else if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == FLOAT)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == FLOAT) {
            return check float:fromString(<string>value);

        } else if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == DECIMAL)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == DECIMAL) {
            return check decimal:fromString(<string>value);

        } else if (fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == BOOLEAN)
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

        } else if (fieldMetaData is SimpleFieldMetadata && (fieldMetaData[FIELD_DATA_TYPE] == CIVIL))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == CIVIL) {
            return self.stringToTime(<string>value, CIVIL);

        } else if (fieldMetaData is SimpleFieldMetadata && (fieldMetaData[FIELD_DATA_TYPE] == UTC))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == UTC) {
            return self.stringToTime(<string>value, UTC);

        } else {
            return error persist:Error("Unsupported Data Format");
        }
    }

    isolated function logQuery(RedisDBOperation msgTag, string|string[]|map<string> metadata) {

        string info = "";
        if metadata is string {
            match msgTag {
                KEYS => {
                    info = string `Pattern : ${metadata}`;
                }
                REDISTYPE|SCARD|SMEMBERS => {
                    info = string `Key : ${metadata}`;
                }
            }
        } else if metadata is string[] {
            match msgTag {
                EXISTS|DEL => {
                    info = string `Keys : ${metadata.toString()}`;
                }
            }
        } else {
            match msgTag {
                HMSET => {
                    info = string `Key : ${metadata["key"] ?: ""}, Record : ${metadata["record"] ?: ""}`;
                }
                SREM => {
                    info = string `Remove ${metadata["key"] ?: ""} from the set ${metadata["suffixes"] ?: ""}`;
                }
                HSET => {
                    info = string `Key : ${metadata["key"] ?: ""}, FieldName : ${metadata["fieldName"] ?: ""},
                     New Value : ${metadata["newValue"] ?: ""}`;
                }
                SADD => {
                    info = string `Key : ${metadata["key"] ?: ""}, Element : ${metadata["suffixes"] ?: ""}`;
                }
                HMGET => {
                    info = string `Key : ${metadata["key"] ?: ""}, FieldNames : ${metadata["fieldNames"] ?: ""}`;
                }
            }
        }
        log:printDebug(string `<Redis ${msgTag.toString().toUpperAscii()} Operation> ${info}`);
    }
}
