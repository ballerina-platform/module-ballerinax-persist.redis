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

import ballerina/persist;

public class PersistRedisStream {

    private stream<record {}, error?>? anydataStream;
    private persist:Error? err;
    private string[] fields;
    private string[] include;
    private typedesc<record {}>[] typeDescriptions;
    private RedisClient? persistClient;
    private typedesc<record {}> targetType;
    private map<anydata> typeMap;

    public isolated function init(stream<record {}, error?>? anydataStream, typedesc<record {}> targetType, map<anydata> typeMap, string[] fields, string[] include, any[] typeDescriptions, RedisClient persistClient, persist:Error? err = ()) {
        self.anydataStream = anydataStream;
        self.fields = fields;
        self.include = include;
        self.targetType = targetType;
        self.typeMap = typeMap;

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
            return self.err;
        } else if self.anydataStream is stream<record {}, persist:Error?> {
            var anydataStream = <stream<record {}, error?>>self.anydataStream;
            var streamValue = anydataStream.next();
            if streamValue is () {
                return streamValue;
            } else if (streamValue is error) {
                return error persist:Error(streamValue.message());
            } else {
                record {}|error value = streamValue.value;
                if value is error {
                    return error persist:Error(value.message());
                }
                check (<RedisClient>self.persistClient).getManyRelations(self.typeMap, value, self.fields, 
                self.include);

                string[] keyFields = (<RedisClient>self.persistClient).getKeyFields();
                foreach string keyField in keyFields {
                    if self.fields.indexOf(keyField) is () && value.hasKey(keyField) {
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
        (stream<anydata, error?>)? str = self.anydataStream;
        if str is stream<anydata, error?> {
            error? e = str.close();
            if e is error {
                return error persist:Error(e.message());
            }
        }
    }
}
