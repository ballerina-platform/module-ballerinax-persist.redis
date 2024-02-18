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
            return check 'object.cloneWithType(rowType);
        } on fail error e {
            if e is persist:NotFoundError {
                return e;
            }
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
        string[]|error keys = self.dbClient->lRange(self.collectionName, 0, -1);
        if keys is error {
            return error persist:Error(keys.message());
        }

        // Get records one by one using the key
        record {}[] result = [];
        foreach string key in keys {
            // Handling simple fields
            string recordKey = string `${self.collectionName}${key}`;
            record {} 'object = check self.querySimpleFieldsByKey(typeMap, recordKey, fields);
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
            record {} newInsertRecord = check self.newRecordWithDateTime(insertRecord);

            // Generate the key
            string key = "";
            foreach string keyField in self.keyFields {
                key += string `${KEY_SEPERATOR}${insertRecord[keyField].toString()}`;
            }

            // Check for duplicate keys withing the collection
            int isKeyExists = check self.dbClient->exists([string `${self.collectionName}${key}`]);
            if isKeyExists != 0 {
                return persist:getAlreadyExistsError(self.collectionName, string `${self.collectionName}${key}`);
            }

            // Check for any relation field constraints
            persist:Error? checkConstraints = self.checkRelationFieldConstraints(key, insertRecord);
            if checkConstraints is persist:ConstraintViolationError{
                return checkConstraints;
            }

            // Insert the record
            lock {
                result = self.dbClient->hMSet(string `${self.collectionName}${key}`, newInsertRecord);
                if result is error {
                    return error persist:Error(result.message());
                }

                // Insert the key to a list to preserve insertion order
                _ = check self.dbClient->rPush(self.collectionName, [key]);
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
        string recSuffix = string `${self.getKey(key)}`;
        // Delete the record
        lock {
            do {
                // Check for references
                string[] allRefFields = [];
                foreach RefMetadata refMedaData in self.refMetadata{
                    allRefFields.push(...refMedaData.joinFields);
                }

                // Remove any references if exists
                if allRefFields.length() > 0 {
                    map<any> currentObject = check self.dbClient->hMGet(
                        string `${self.collectionName}${recSuffix}`,allRefFields);
                    foreach RefMetadata refMedaData in self.refMetadata{
                        string refKey = refMedaData.refCollection;
                        foreach string refField in refMedaData.joinFields {
                            refKey += string `${KEY_SEPERATOR}${currentObject[refField].toString()}`;
                        }
                        _ = check self.dbClient->sRem(string `${refKey}${KEY_SEPERATOR}${self.collectionName}`, [recSuffix]);
                    }
                }

                // Remove the record from the Collection list
                _ = check self.dbClient->lRem(self.collectionName, 1, recSuffix);
                // Remove the record
                _ = check self.dbClient->del([string `${self.collectionName}${recSuffix}`]);
            } on fail error e {
                return error persist:Error(e.message());
            }
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

        // Verify the existence of the key
        do {
	        int isKeyExists = check self.dbClient->exists([recordKey]);
            if isKeyExists == 0 {
                return persist:getNotFoundError(self.collectionName, recordKey);
            }
        } on fail var e {
        	return error persist:Error(e.message());
        }

        // Convert time:Date and time:TimeOfDay to string
        record {} newUpdateRecord = check self.newRecordWithDateTime(updateRecord);

        // Get the original record before update
        map<()> updatedEntities = {};
        map<any>|error prevRecord = self.dbClient->hMGet(recordKey, newUpdateRecord.keys());
        if prevRecord is error {
            return error persist:Error(prevRecord.message());
        }

        // Check the validity of new associations
        foreach RefMetadata refMetaData in self.refMetadata{
            string[] joinFields = refMetaData.joinFields;

            // Recreate the key
            string relatedRecordKey = refMetaData.refCollection;
            foreach string joinField in joinFields{
                if newUpdateRecord.hasKey(joinField){
                    updatedEntities[refMetaData.refCollection] = ();
                    relatedRecordKey += string `${KEY_SEPERATOR}${newUpdateRecord[joinField].toString()}`;
                }else{
                    relatedRecordKey += string `${KEY_SEPERATOR}${prevRecord[joinField].toString()}`;
                }
            }

            // Verify the new associated entities does exists
            if updatedEntities.hasKey(refMetaData.refCollection){
                int isKeyExists = check self.dbClient->exists([relatedRecordKey]);
                if isKeyExists != 0 {
                    // Verify the key type as a HASH
                    string redisType = check self.dbClient->redisType(relatedRecordKey);
                    if redisType == REDIS_HASH {
                        continue;
                    }
                }
                // Return a constrain violation error if new associations does not exists
                return getConstraintViolationError(self.collectionName, refMetaData.refCollection);
            }
        } on fail error e {
        	return error persist:Error(e.message());
        }

        // Verify the availablity of new associations
        // Eg: Reffered record might already in a ONE-TO-ONE relationship
        foreach RefMetadata refMetaData in self.refMetadata{
            if !updatedEntities.hasKey(refMetaData.refCollection) {
                continue;
            }
            string[] joinFields = refMetaData.joinFields;
            string newRelatedRecordKey = refMetaData.refCollection;
            foreach string joinField in joinFields{
                if newUpdateRecord.hasKey(joinField){
                    newRelatedRecordKey += string `${KEY_SEPERATOR}${newUpdateRecord[joinField].toString()}`;
                }else{
                    newRelatedRecordKey += string `${KEY_SEPERATOR}${prevRecord[joinField].toString()}`;
                }
            }

            // Get keys of existing associations
            int isKeyExists = check self.dbClient->exists([
                string `${newRelatedRecordKey}${KEY_SEPERATOR}${self.collectionName}`]);
            if isKeyExists != 0 {
                // Verify the key type as a SET
                string redisType = check self.dbClient->redisType(
                    string `${newRelatedRecordKey}${KEY_SEPERATOR}${self.collectionName}`);
                if redisType == REDIS_SET {
                    // Check existing associations for ONE-TO-ONE
                    if refMetaData.'type == ONE_TO_ONE {
                        int cardinality = check self.dbClient->sCard(
                            string `${newRelatedRecordKey}${KEY_SEPERATOR}${self.collectionName}`);
                        if cardinality > 0 {
                            return getConstraintViolationError(self.collectionName, refMetaData.refCollection);
                        }
                    } 
                }
            }
        } on fail error e {
        	return error persist:Error(e.message());
        }

        // Update
        lock {
            foreach string updatedField in newUpdateRecord.keys(){
                _ = check self.dbClient->hSet(recordKey, updatedField, 
                        newUpdateRecord[updatedField].toString());
            } on fail error e {
            	return error persist:Error(e.message());
            }

            // Add new association to the SET
            foreach RefMetadata refMetaData in self.refMetadata{
                if !updatedEntities.hasKey(refMetaData.refCollection){
                    continue;
                } 
                string[] joinFields = refMetaData.joinFields;
                string newRelatedRecordKey = refMetaData.refCollection;
                foreach string joinField in joinFields{
                    if newUpdateRecord.hasKey(joinField){
                        newRelatedRecordKey += string `${KEY_SEPERATOR}${newUpdateRecord[joinField].toString()}`;
                    }else{
                        newRelatedRecordKey += string `${KEY_SEPERATOR}${prevRecord[joinField].toString()}`;
                    }
                }
                int|error sAdd = self.dbClient->sAdd(
                    string `${newRelatedRecordKey}${KEY_SEPERATOR}${self.collectionName}`, [recordKey]);
                if sAdd is error {
                    return error persist:Error(sAdd.message());
                }
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
            string[]|error keySuffixes = self.getRelatedEntityKeySuffixes(entity,'object);
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
        foreach string keyField in self.keyFields{
            key += string `${KEY_SEPERATOR}${'object[keyField].toString()}`;
        }
        return key;
    }

    private isolated function getRelatedEntityKeySuffixes(string entity, record {} 'object) returns string []|error {
        (RefMetadata & readonly)? refMetaData = self.refMetadata[entity];
        if refMetaData == () {
            return [];
        }

        do {
            if refMetaData.joinFields == self.keyFields {
                // Non-owner have direct access to the association set
                string[] keys = check self.dbClient->sMembers(
                    string `${self.getKeyFromObject('object)}${KEY_SEPERATOR}${refMetaData.refCollection}`);
                return keys;
            }else{
                map<any> recordWithRefFields = check self.dbClient->hMGet(self.getKeyFromObject('object), 
                refMetaData.joinFields);
                string key = "";
                foreach string joinField in refMetaData.joinFields{
                    key += string `${KEY_SEPERATOR}${recordWithRefFields[joinField].toString()}`;
                }
                return [key];
            }
        } on fail error e {
            return e;
        }
    }

    private isolated function querySimpleFieldsByKey(map<anydata> typeMap, string key, string[] fields) 
    returns record {|anydata...;|}|persist:Error {
        string[] simpleFields = self.getTargetSimpleFields(fields, typeMap);

        do {
            // Retrieve the record
            map<any> value = check self.dbClient->hMGet(key, simpleFields);
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
            return <persist:Error>e.cause();
        }
    }

    private isolated function queryRelationFieldsByKey(string entity, CardinalityType cardinalityType, string key, string[] fields) returns record {|anydata...;|}|persist:Error {
        string[] relationFields = fields.clone();
        (RefMetadata & readonly)? refMetaData = self.refMetadata[entity];
        if refMetaData == () {
            return error persist:Error(string `Undefined relation between ${self.entityName} and ${entity}`);
        }

        // Add required missing reference fields
        foreach string refKeyField in refMetaData.refFields{
            if relationFields.indexOf(refKeyField) is () {
                relationFields.push(refKeyField);
            }
        }

        do {
            // Retrieve related records
            map<any> value = check self.dbClient->hMGet(key, relationFields);
            if self.isNoRecordFound(value) {
                return error persist:Error(string `No '${self.entityName}' found for the given key '${key}'`);
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
                    <FieldMetadata & readonly>self.fieldMetadata[string `${fieldMetadataKeyPrefix}${fieldKey}`], <anydata>value[fieldKey]);
            }
            return valueToRecord;
        } on fail error e {
            return error persist:Error(e.message());
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
                int|error sCard = self.dbClient->sCard(
                    string `${refRecordKey}${KEY_SEPERATOR}${self.collectionName}`);
                if sCard is int && sCard > 0 && refMetadataValue.'type == ONE_TO_ONE {
                    // If the refered record is already in an association
                    return getConstraintViolationError(self.collectionName, refMetadataValue.refCollection);
                }

                // Associate current record with the refered record
                int|error sAdd = self.dbClient->sAdd(
                    string `${refRecordKey}${KEY_SEPERATOR}${self.collectionName}`, [key]);
                if sAdd is error {
                    return error persist:Error(sAdd.message());
                }

                map<any> value = check self.dbClient->hMGet(refRecordKey, refMetadataValue.refFields);
                if self.isNoRecordFound(value) {
                    return getConstraintViolationError(self.collectionName, refMetadataValue.refCollection);
                }
            } on fail error e {
                return error persist:Error(e.message());
            }
        }
    }

    private isolated function newRecordWithDateTime(record {} insertRecord) returns record {}|persist:Error {
        record {} newRecord = {};
        foreach string recordfield in insertRecord.keys() {
            (FieldMetadata & readonly)? fieldMetaDataValue = self.fieldMetadata[recordfield];
            if fieldMetaDataValue is SimpleFieldMetadata {
                DataType dataType = fieldMetaDataValue.fieldDataType;

                if dataType == DATE || dataType == TIME_OF_DAY {
                    RedisTimeType?|error timeValue = insertRecord.get(recordfield).ensureType();
                    if timeValue is error {
                        return error persist:Error(timeValue.message());
                    }

                    string|persist:Error? timeValueInString = self.timeToString(timeValue);
                    if timeValueInString is persist:Error {
                        return timeValueInString;
                    }

                    newRecord[recordfield] = timeValueInString;
                }else{
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
            time:TimeOfDay output = {hour: check int:fromString(timeValues[0]), minute: check int:fromString(
                timeValues[1]), second: check decimal:fromString(timeValues[2])};
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
            return check int:fromString(value.toString());

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
