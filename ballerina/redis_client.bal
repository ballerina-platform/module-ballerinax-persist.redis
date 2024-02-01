import ballerinax/redis;
import ballerina/persist;
// import ballerina/io;

# The client used by the generated persist clients to abstract and 
# execute Redis queries that are required to perform CRUD operations.
public isolated client class RedisClient {

    private final redis:Client dbClient;

    private final string & readonly entityName;
    private final string & readonly collectionName;
    private final map<FieldMetadata> & readonly fieldMetadata;
    private final string[] & readonly keyFields;

    # Initializes the `RedisClient`.
    #
    # + dbClient - The `redis:Client`, which is used to execute Redis queries
    # + metadata - Metadata of the entity
    # + return - A `persist:Error` if the client creation fails
    public isolated function init(redis:Client dbClient, RedisMetadata & readonly metadata) returns persist:Error? {
        self.entityName = metadata.entityName;
        self.collectionName = metadata.collectionName;
        self.fieldMetadata = metadata.fieldMetadata;
        self.keyFields = metadata.keyFields;
        self.dbClient = dbClient;
    }

    # Performs a batch `HGET` operation to get entity instances as a stream
    # 
    # + rowType - The type description of the entity to be retrieved
    # + typeMap - The data type map of the target type
    # + key - Key for the record
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + typeDescriptions - The type descriptions of the relations to be retrieved
    # + return - An `record{||} & readonly` containing the requested record
    public isolated function runReadByKeyQuery(typedesc<record {}> rowType, map<anydata> typeMap, anydata key, string[] fields = [], string[] include = [], typedesc<record {}>[] typeDescriptions = []) returns record {|anydata...;|}|error {
        string recordKey = self.collectionName;
        // assume the key fields are in the same order as when inserting a new record
        recordKey += self.getKey(key);
        do {
            // handling simple fields
            record{} 'object = check self.querySimpleFieldsByKey(typeMap, recordKey, fields);
            // handling relation fields
            check self.getManyRelations(typeMap, 'object, fields, include);

            self.removeUnwantedFields('object, fields);
            self.removeNonExistOptionalFields('object);
            return check 'object.cloneWithType(rowType);
        } on fail error e {
            return <persist:Error>e;
        }
    }

    # Performs a batch `HGET` operation to get entity instances as a stream
    # 
    # + rowType - The type description of the entity to be retrieved
    # + typeMap - The data types of the record
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of `record{||} & readonly` containing the requested records
    public isolated function runReadQuery(typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [], string[] include = []) returns stream<record{}|error?>|persist:Error {
        // Get all the keys
        string[]|error keys = self.dbClient->keys(self.collectionName+":*");
        if keys is error {
            return error persist:Error(keys.message());
        }

        // Get data one by one using the key
        record{}[] result = [];
        foreach string key in keys {
            // handling simple fields only for batch read
            record{} 'object = check self.querySimpleFieldsByKey(typeMap, key, fields);
            
            self.removeUnwantedFields('object, fields);
            self.removeNonExistOptionalFields('object);
            result.push('object);
            
        }

        return stream from record{} rec in result select rec;
    }

    # Performs a batch `HMSET` operation to insert entity instances into a collection.
    #
    # + insertRecords - The entity records to be inserted into the collection
    # + return - A `string` containing the information of the query execution
    # or a `persist:Error` if the operation fails
    public isolated function runBatchInsertQuery(record {}[] insertRecords) returns string|persist:Error|error {

        string|error result;

        // for each record do HMSET
        foreach var insertRecord in insertRecords {

            // Create the key
            string key = "";
            foreach string keyField in self.keyFields {
                key = key + ":" + insertRecord[keyField].toString(); // get the key field value by member access method.
            }

            // check for duplicate keys withing the collection
            int isKeyExists = check self.dbClient->exists([self.collectionName+key]);
            if isKeyExists != 0 {
                return persist:getAlreadyExistsError(self.collectionName, key);
            }

            // inserting the object
            result = self.dbClient->hMSet(self.collectionName+key, insertRecord);
            if result is error{
                return error persist:Error(result.message());
            }
        }

        // Decide how to log queries
        if result is string {
            return result;
        }
        return error persist:Error(result.message());
    }

    # Performs redis `DEL` operation to delete an entity record from the database.
    #
    # + keyFieldValues - The ordered keys used to delete an entity record
    # + return - `()` if the operation is performed successfully or a `persist:Error` if the operation fails
    public isolated function runDeleteQuery(any [] keyFieldValues) returns persist:Error?|error {
        // Validate fields
        if (keyFieldValues.length() != self.keyFields.length()){
            return error("Missing keyfields");
        }

        // Generate the key
        string recordKey = self.collectionName;
        foreach any value in keyFieldValues{
            recordKey += ":"+value.toString();
        }

        // Delete the record
        _ = check self.dbClient->del([recordKey]);
    }

    # Performs redis `HSET` operation to delete an entity record from the database.
    #
    # + keyFieldValues - The ordered keys used to update an entity record
    # + updateRecord - The new record to be updated
    # + return - An Error if the new record is missing a keyfield
    public isolated function runUpdateQuery(any [] keyFieldValues, record {} updateRecord) returns error? {
        // Validate fields
        if (keyFieldValues.length() != self.keyFields.length()){
            return error("Missing keyfields");
        }

        // Generate the key
        string key = self.collectionName;
        foreach any keyFieldValue in keyFieldValues{
            key += ":"+keyFieldValue.toString();
        }

        // decide on how to update only the given fields that is not equals to ()
        foreach [string, FieldMetadata & readonly] metaDataEntry in self.fieldMetadata.entries() {
            FieldMetadata & readonly fieldMetadataValue = metaDataEntry[1];

            // if the field is a simple field
            if(fieldMetadataValue is SimpleFieldMetadata){
                if (updateRecord.hasKey(fieldMetadataValue.fieldName) && updateRecord[fieldMetadataValue.fieldName] != ()){
                    // updating the object
                    _ = check self.dbClient->hSet(key, fieldMetadataValue.fieldName, updateRecord[fieldMetadataValue.fieldName].toString());
                }
            }
            // if the field is a relation field
            else{

            }
        }
    }

    public isolated function getKeyFields() returns string[] {
        return self.keyFields;
    }

    private isolated function getKey(anydata key) returns string {
        string keyValue = "";
        if key is map<any> {
            foreach string compositeKey in key.keys(){
                keyValue += ":"+key[compositeKey].toString();
            }
            return keyValue;
        } else {
            return ":"+key.toString();
        }
    }

    public isolated function getManyRelations(map<anydata> typeMap, record {} 'object, string[] fields, string[] include) returns persist:Error? {
        foreach int i in 0 ..< include.length() {
            string entity = include[i];

            JoinType joinType = ONE_TO_MANY;
            // checking for one to many relationships
            string[] relationFields = from string 'field in fields
                where 'field.startsWith(entity + "[].")
                select 'field.substring(entity.length() + 3, 'field.length());
            // checking for one to one relationships
            if relationFields.length() == 0 {

                relationFields = from string 'field in fields
                where 'field.startsWith(entity + ".")
                select 'field.substring(entity.length() + 1, 'field.length());
                if relationFields.length() != 0{
                    joinType = ONE_TO_ONE;
                }
            }

            if relationFields.length() is 0 {
                continue;
            }

            string[]keys = check self.dbClient->keys(entity.substring(0,1).toUpperAscii()+entity.substring(1)+":*");

            // Get data one by one using the key
            record{}[] associatedRecords = [];
            foreach string key in keys {
                // handling simple fields
                record{} valueToRecord = check self.queryRelationFieldsByKey(entity, joinType, key, relationFields);

                // check whether the record is associated with the current object
                boolean isAssociated = true;
                foreach string keyField in self.keyFields{
                    string refField = self.entityName.substring(0,1).toLowerAscii()+self.entityName.substring(1)
                    +keyField.substring(0,1).toUpperAscii()+keyField.substring(1);
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
            
            if(joinType == ONE_TO_ONE && associatedRecords.length() != 0){
                'object[entity] = associatedRecords[0];
            }else{
                'object[entity] = associatedRecords;
            }
        } on fail var e {
        	return <persist:Error>e;
        }
    }

    private isolated function querySimpleFieldsByKey(map<anydata> typeMap, string key, string[] fields) returns record {|anydata...;|}|persist:Error{
        // hadling the simple fields
        string[] simpleFields = self.getTargetSimpleFields(fields, typeMap);
        if simpleFields == [] { // then add all the fields by default
            foreach [string, FieldMetadata & readonly] metaDataEntry in self.fieldMetadata.entries() {
                FieldMetadata & readonly fieldMetadataValue = metaDataEntry[1];

                // if the field is a simple field
                if(fieldMetadataValue is SimpleFieldMetadata){
                    simpleFields.push(fieldMetadataValue.fieldName);
                }
            }
        }

        do {
	
	        map<any> value = check self.dbClient->hMGet(key, simpleFields);
            if self.isNoRecordFound(value) {
                return error persist:Error("No "+self.entityName+" found for the given key");
            }
            record{} valueToRecord = {};
            foreach string fieldKey in value.keys() {
                // convert the data type from 'any' to required type
                valueToRecord[fieldKey] = check self.dataConverter(
                    <FieldMetadata & readonly>self.fieldMetadata[fieldKey]
                    , value[fieldKey]);
            }
            return valueToRecord;
        } on fail var e {
        	return <persist:Error>e;
        }
    }

    private isolated function queryRelationFieldsByKey(string entity, JoinType joinType, string key, string[] fields) returns record {|anydata...;|}|persist:Error{
        // if the field doesn't containes reference fields
        // add them here
        string[] relationFields = fields.clone();
        foreach string keyField in self.keyFields {
            string refField = self.entityName.substring(0,1).toLowerAscii()+self.entityName.substring(1)
            +keyField.substring(0,1).toUpperAscii()+keyField.substring(1);
            if relationFields.indexOf(refField) is () {
                relationFields.push(refField);
            }
        }

        do {
	        map<any> value = check self.dbClient->hMGet(key, relationFields);
            if self.isNoRecordFound(value) {
                return error persist:Error("No "+self.entityName+" found for the given key");
            }

            record{} valueToRecord = {};

            string fieldMetadataKeyPrefix = entity;
            if(joinType == ONE_TO_MANY){
                fieldMetadataKeyPrefix += "[].";
            }else{
                fieldMetadataKeyPrefix += ".";
            }

            foreach string fieldKey in value.keys() {
                // convert the data type from 'any' to required type
                valueToRecord[fieldKey] = check self.dataConverter(
                    <FieldMetadata & readonly>self.fieldMetadata[fieldMetadataKeyPrefix+fieldKey]
                    , value[fieldKey]);
            }
            return valueToRecord;
        } on fail var e {
        	return <persist:Error>e;
        }
    }

    private isolated function getTargetSimpleFields(string[] fields, map<anydata> typeMap) returns string[] {
        string[] simpleFields = from string 'field in fields
            where !'field.includes(".") && typeMap.hasKey('field)
            select 'field;
        return simpleFields;
    }


    private isolated function removeNonExistOptionalFields(record {} 'object){
        foreach string key in 'object.keys(){
            if 'object[key] == () {
                _ = 'object.remove(key);
            }
        }
    }

    private isolated function isNoRecordFound(map<any> value) returns boolean{
        boolean isNoRecordExists = true;
        foreach string key in value.keys(){
            if value[key] != () {
                isNoRecordExists = false;
            }
        }
        return isNoRecordExists;
    }

    private isolated function removeUnwantedFields(record {} 'object, string[] fields) {
        string[] keyFields = self.keyFields;

        foreach string keyField in keyFields {
            if fields.indexOf(keyField) is () {
                _ = 'object.remove(keyField);
            }
        }
    }

    private isolated function dataConverter(FieldMetadata & readonly fieldMetaData, any value) returns ()|boolean|string|float|error|int {

        // Return nil if value is nil
        if(value is ()){
            return ();
        }
    
        if((fieldMetaData is SimpleFieldMetadata && fieldMetaData[FIELD_DATA_TYPE] == INT)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == INT)){
            return check int:fromString(<string>value);
        }else if((fieldMetaData is SimpleFieldMetadata  && (fieldMetaData[FIELD_DATA_TYPE] == STRING))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == STRING)){
            return <string>value;
        }else if((fieldMetaData is SimpleFieldMetadata  && fieldMetaData[FIELD_DATA_TYPE] == FLOAT)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == FLOAT)){
            return check float:fromString(<string>value);
        }else if((fieldMetaData is SimpleFieldMetadata  && fieldMetaData[FIELD_DATA_TYPE] == BOOLEAN)
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == BOOLEAN)){
            return check boolean:fromString(<string>value);
        }else if((fieldMetaData is SimpleFieldMetadata  && (fieldMetaData[FIELD_DATA_TYPE] == ENUM))
        || (fieldMetaData is EntityFieldMetadata && fieldMetaData[RELATION][REF_FIELD_DATA_TYPE] == ENUM)){
            return <string>value;
        }else{
            return error("Unsupported Data Format");
        }
    }

}
