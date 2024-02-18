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

import ballerina/test;
import ballerina/persist;

@test:Config {
    groups: ["all-types", "redis"]
}
function redisAllTypesCreateTest() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    int[] ids = check testEntitiesClient->/alltypes.post([allTypes1, allTypes2]);
    test:assertEquals(ids, [allTypes1.id, allTypes2.id]);

    AllTypes allTypesRetrieved = check testEntitiesClient->/alltypes/[allTypes1.id].get();
    test:assertEquals(allTypesRetrieved, allTypes1Expected);

    allTypesRetrieved = check testEntitiesClient->/alltypes/[allTypes2.id].get();
    test:assertEquals(allTypesRetrieved, allTypes2Expected);

    check testEntitiesClient.close();
}

@test:Config {
    groups: ["all-types", "redis"]
}
function redisAllTypesCreateOptionalTest() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    int[] ids = check testEntitiesClient->/alltypes.post([allTypes3]);
    test:assertEquals(ids, [allTypes3.id]);

    AllTypes allTypesRetrieved = check testEntitiesClient->/alltypes/[allTypes3.id].get();
    test:assertEquals(allTypesRetrieved, allTypes3Expected);

    check testEntitiesClient.close();
}

@test:Config {
    groups: ["all-types", "redis"],
    dependsOn: [redisAllTypesCreateTest, redisAllTypesCreateOptionalTest]
}
function redisAllTypesReadTest() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    stream<AllTypes, error?> allTypesStream = testEntitiesClient->/alltypes.get();
    AllTypes[] allTypes = check from AllTypes allTypesRecord in allTypesStream
        select allTypesRecord;

    test:assertEquals(allTypes, [allTypes3Expected, allTypes1Expected, allTypes2Expected]);
    check testEntitiesClient.close();
}

@test:Config {
    groups: ["all-types", "redis", "dependent"],
    dependsOn: [redisAllTypesCreateTest, redisAllTypesCreateOptionalTest]
}
function redisAllTypesReadDependentTest() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    stream<AllTypesDependent, error?> allTypesStream = testEntitiesClient->/alltypes.get();
    AllTypesDependent[] allTypes = check from AllTypesDependent allTypesRecord in allTypesStream
        select allTypesRecord;

    test:assertEquals(allTypes, [
        {
            booleanType: allTypes3Expected.booleanType,
            intType: allTypes3Expected.intType,
            floatType: allTypes3Expected.floatType,
            decimalType: allTypes3Expected.decimalType,
            stringType: allTypes3Expected.stringType,
            dateType: allTypes3Expected.dateType,
            timeOfDayType: allTypes3Expected.timeOfDayType
        },
        {
            booleanType: allTypes1Expected.booleanType,
            intType: allTypes1Expected.intType,
            floatType: allTypes1Expected.floatType,
            decimalType: allTypes1Expected.decimalType,
            stringType: allTypes1Expected.stringType,
            dateType: allTypes1Expected.dateType,
            timeOfDayType: allTypes1Expected.timeOfDayType,
            booleanTypeOptional: allTypes1Expected.booleanTypeOptional,
            intTypeOptional: allTypes1Expected.intTypeOptional,
            floatTypeOptional: allTypes1Expected.floatTypeOptional,
            decimalTypeOptional: allTypes1Expected.decimalTypeOptional,
            stringTypeOptional: allTypes1Expected.stringTypeOptional,
            dateTypeOptional: allTypes1Expected.dateTypeOptional,
            timeOfDayTypeOptional: allTypes1Expected.timeOfDayTypeOptional
        },
        {
            booleanType: allTypes2Expected.booleanType,
            intType: allTypes2Expected.intType,
            floatType: allTypes2Expected.floatType,
            decimalType: allTypes2Expected.decimalType,
            stringType: allTypes2Expected.stringType,
            dateType: allTypes2Expected.dateType,
            timeOfDayType: allTypes2Expected.timeOfDayType,
            booleanTypeOptional: allTypes2Expected.booleanTypeOptional,
            intTypeOptional: allTypes2Expected.intTypeOptional,
            floatTypeOptional: allTypes2Expected.floatTypeOptional,
            decimalTypeOptional: allTypes2Expected.decimalTypeOptional,
            stringTypeOptional: allTypes2Expected.stringTypeOptional,
            dateTypeOptional: allTypes2Expected.dateTypeOptional,
            timeOfDayTypeOptional: allTypes2Expected.timeOfDayTypeOptional
        }
    ]);
    check testEntitiesClient.close();
}

@test:Config {
    groups: ["all-types", "redis"],
    dependsOn: [redisAllTypesCreateTest, redisAllTypesCreateOptionalTest]
}
function redisAllTypesReadOneTest() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    AllTypes allTypesRetrieved = check testEntitiesClient->/alltypes/[allTypes1.id].get();
    test:assertEquals(allTypesRetrieved, allTypes1Expected);

    allTypesRetrieved = check testEntitiesClient->/alltypes/[allTypes2.id].get();
    test:assertEquals(allTypesRetrieved, allTypes2Expected);

    allTypesRetrieved = check testEntitiesClient->/alltypes/[allTypes3.id].get();
    test:assertEquals(allTypesRetrieved, allTypes3Expected);

    check testEntitiesClient.close();
}

@test:Config {
    groups: ["all-types", "redis"]
}
function redisAllTypesReadOneTestNegative() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    AllTypes|persist:Error allTypesRetrieved = testEntitiesClient->/alltypes/[4].get();
    if allTypesRetrieved is persist:NotFoundError {
        test:assertEquals(allTypesRetrieved.message(), "A record with the key 'AllTypes:4' does not exist for the entity 'AllTypes'.");
    }
    else {
        test:assertFail("persist:NotFoundError expected.");
    }

    check testEntitiesClient.close();
}

@test:Config {
    groups: ["all-types", "redis"],
    dependsOn: [redisAllTypesReadOneTest, redisAllTypesReadTest, redisAllTypesReadDependentTest]
}
function redisAllTypesUpdateTest() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    AllTypes allTypes = check testEntitiesClient->/alltypes/[allTypes1.id].put({
        booleanType: allTypes3.booleanType,
        intType: allTypes1Updated.intType,
        floatType: allTypes1Updated.floatType,
        decimalType: allTypes1Updated.decimalType,
        stringType: allTypes1Updated.stringType,
        dateType: allTypes1Updated.dateType,
        timeOfDayType: allTypes1Updated.timeOfDayType,
        booleanTypeOptional: allTypes1Updated.booleanTypeOptional,
        intTypeOptional: allTypes1Updated.intTypeOptional,
        floatTypeOptional: allTypes1Updated.floatTypeOptional,
        decimalTypeOptional: allTypes1Updated.decimalTypeOptional,
        stringTypeOptional: allTypes1Updated.stringTypeOptional,
        dateTypeOptional: allTypes1Updated.dateTypeOptional,
        timeOfDayTypeOptional: allTypes1Updated.timeOfDayTypeOptional,
        enumType: allTypes1Updated.enumType,
        enumTypeOptional: allTypes1Updated.enumTypeOptional
    });
    test:assertEquals(allTypes, allTypes1UpdatedExpected);

    AllTypes allTypesRetrieved = check testEntitiesClient->/alltypes/[allTypes1.id].get();
    test:assertEquals(allTypesRetrieved, allTypes1UpdatedExpected);
    check testEntitiesClient.close();
}

@test:Config {
    groups: ["all-types", "redis"],
    dependsOn: [redisAllTypesUpdateTest]
}
function redisAllTypesDeleteTest() returns error? {
    RedisTestEntitiesClient testEntitiesClient = check new ();

    AllTypes allTypes = check testEntitiesClient->/alltypes/[allTypes2.id].delete();
    test:assertEquals(allTypes, allTypes2Expected);

    stream<AllTypes, error?> allTypesStream = testEntitiesClient->/alltypes.get();
    AllTypes[] allTypesCollection = check from AllTypes allTypesRecord in allTypesStream
        select allTypesRecord;

    test:assertEquals(allTypesCollection, [allTypes3Expected, allTypes1UpdatedExpected]);
    check testEntitiesClient.close();
}
