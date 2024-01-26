import ballerina/persist;

public class PersistRedisStream {

    private stream<record {}, error?>? anydataStream;
    private persist:Error? err;
    private string[] fields;
    private string[] include;
    private typedesc<record {}>[] typeDescriptions;
    private RedisClient? persistClient;
    private typedesc<record {}> targetType;

    public isolated function init(stream<record {}, error?>? anydataStream, typedesc<record {}> targetType, string[] fields, string[] include, any[] typeDescriptions, RedisClient persistClient, persist:Error? err = ()) {
        self.anydataStream = anydataStream;
        self.fields = fields;
        self.include = include;
        self.targetType = targetType;

        typedesc<record {}>[] typeDescriptionsArray = [];
        foreach any typeDescription in typeDescriptions {
            typeDescriptionsArray.push(<typedesc<record {}>>typeDescription);
        }
        self.typeDescriptions = typeDescriptionsArray;

        self.persistClient = persistClient;
        self.err = err;
    }

    public isolated function next() returns record {|record {} value;|}|persist:Error? {
        if self.err is persist:Error {
            return <persist:Error>self.err;
        } else if self.anydataStream is stream<record {}, persist:Error?> {
            var anydataStream = <stream<record {}, error?>>self.anydataStream;
            var streamValue = anydataStream.next();
            if streamValue is () {
                return streamValue;
            } else if (streamValue is error) {
                return <persist:Error>error(streamValue.message());
            } else {
                record {}|error value = streamValue.value;
                if value is error {
                    return <persist:Error>error(value.message());
                }
                check (<RedisClient>self.persistClient).getManyRelations(value, self.fields, self.include);

                string[] keyFields = (<RedisClient>self.persistClient).getKeyFields();
                foreach string keyField in keyFields {
                    if self.fields.indexOf(keyField) is () {
                        _ = value.remove(keyField);
                    }
                }
                record {|record {} value;|} nextRecord = {value: checkpanic value.cloneWithType(self.targetType)};
                return nextRecord;
            }
        } else {
            return ();
        }
    }

    public isolated function close() returns persist:Error? {
        if self.anydataStream is stream<anydata, error?> {
            error? e = (<stream<anydata, error?>>self.anydataStream).close();
            if e is error {
                return <persist:Error>error(e.message());
            }
        }
    }
}