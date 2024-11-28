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
import ballerina/test;
import ballerina/lang.runtime;
import ballerina/persist;

@test:Config {
    groups: ["cache", "redis"],
    dependsOn: [],
    enable: false
}
function redisAllTest() returns error? {
    RedisCacheClient cacheClient = check new ();

    int[] ids = check cacheClient->/people.post([person1]);
    test:assertEquals(ids, [person1.id]);
    int[] ids2 = check cacheClient->/people.post([person2]);
    test:assertEquals(ids2, [person2.id]);

    string[] apartmentCodes = check cacheClient->/apartments.post([apartment1]);
    test:assertEquals(apartmentCodes, [apartment1.code]);

    PersonWithAssoc personRetrieved = check cacheClient->/people/[person1.id];
    test:assertEquals(personRetrieved, person1WithAssoc);

    ApartmentWithAssoc apartmentRetrieved = check cacheClient->/apartments/[apartment1.code];
    test:assertEquals(apartmentRetrieved, apartment1WithPerson);

    runtime:sleep(4);
    check cacheClient.close();
}

@test:Config {
    groups: ["cache", "redis"],
    dependsOn: [redisAllTest],
    enable: false
}
function redisEmployeeOnlyTest() returns error? {
    RedisCacheClient cacheClient = check new ();

    int[] ids = check cacheClient->/people.post([person1]);
    test:assertEquals(ids, [person1.id]);
    int[] ids2 = check cacheClient->/people.post([person2]);
    test:assertEquals(ids2, [person2.id]);

    string[] apartmentCodes = check cacheClient->/apartments.post([apartment1]);
    test:assertEquals(apartmentCodes, [apartment1.code]);

    runtime:sleep(1);
    _ = check cacheClient->/people/[person1.id].put({name: "Mike"});
    _ = check cacheClient->/people/[person2.id].put({name: "Akela"});
    runtime:sleep(3);

    PersonWithAssoc personRetrieved = check cacheClient->/people/[person1.id];
    test:assertEquals(personRetrieved, person1WithAssocUpdated);

    runtime:sleep(4);
    check cacheClient.close();
}

@test:Config {
    groups: ["cache", "redis"],
    dependsOn: [redisEmployeeOnlyTest],
    enable: false
}
function redisDependentOnlyTest() returns error? {
    RedisCacheClient cacheClient = check new ();

    int[] ids = check cacheClient->/people.post([person1]);
    test:assertEquals(ids, [person1.id]);
    int[] ids2 = check cacheClient->/people.post([person2]);
    test:assertEquals(ids2, [person2.id]);

    string[] apartmentCodes = check cacheClient->/apartments.post([apartment1]);
    test:assertEquals(apartmentCodes, [apartment1.code]);

    runtime:sleep(1);
    _ = check cacheClient->/apartments/[apartment1.code].put({city: "New Jursey"});
    runtime:sleep(3);

    ApartmentWithAssoc|persist:Error apartmentRetrieved = cacheClient->/apartments/[apartment1.code];
    test:assertTrue(apartmentRetrieved is persist:NotFoundError, "Should return a `persist:NotFoundError`");

    runtime:sleep(4);
    check cacheClient.close();
}

@test:Config {
    groups: ["cache", "redis"],
    dependsOn: [redisDependentOnlyTest],
    enable: false
}
function redisEmployeeAndDependentTest() returns error? {
    RedisCacheClient cacheClient = check new ();

    int[] ids = check cacheClient->/people.post([person1]);
    test:assertEquals(ids, [person1.id]);
    int[] ids2 = check cacheClient->/people.post([person2]);
    test:assertEquals(ids2, [person2.id]);

    string[] apartmentCodes = check cacheClient->/apartments.post([apartment1]);
    test:assertEquals(apartmentCodes, [apartment1.code]);

    runtime:sleep(1);
    _ = check cacheClient->/people/[person1.id].put({name: "Mike"});
    _ = check cacheClient->/people/[person2.id].put({name: "Akela"});
    _ = check cacheClient->/apartments/[apartment1.code].put({city: "New Jursey"});
    runtime:sleep(3);

    PersonWithAssoc personRetrieved = check cacheClient->/people/[person1.id];
    test:assertEquals(personRetrieved, person1WithAssocUpdated2);

    ApartmentWithAssoc apartmentRetrieved = check cacheClient->/apartments/[apartment1.code];
    test:assertEquals(apartmentRetrieved, apartment1WithPersonUpdated);

    runtime:sleep(4);
    check cacheClient.close();
}

@test:Config {
    groups: ["cache", "redis"],
    dependsOn: [redisEmployeeAndDependentTest],
    enable: false
}
function redisDependentAndSetTest() returns error? {
    RedisCacheClient cacheClient = check new ();

    int[] ids = check cacheClient->/people.post([person1]);
    test:assertEquals(ids, [person1.id]);
    int[] ids2 = check cacheClient->/people.post([person2]);
    test:assertEquals(ids2, [person2.id]);

    runtime:sleep(1);
    string[] apartmentCodes = check cacheClient->/apartments.post([apartment1]);
    test:assertEquals(apartmentCodes, [apartment1.code]);
    runtime:sleep(3);

    ApartmentWithAssoc|persist:Error apartmentRetrieved = cacheClient->/apartments/[apartment1.code];
    test:assertTrue(apartmentRetrieved is persist:NotFoundError, "Should return a `persist:NotFoundError`");

    runtime:sleep(4);
    check cacheClient.close();
}

@test:Config {
    groups: ["cache", "redis"],
    dependsOn: [redisDependentAndSetTest],
    enable: false
}
function redisEmployeeAndSetTest() returns error? {
    RedisCacheClient cacheClient = check new ();

    int[] ids = check cacheClient->/people.post([person1]);
    test:assertEquals(ids, [person1.id]);
    int[] ids2 = check cacheClient->/people.post([person2]);
    test:assertEquals(ids2, [person2.id]);

    runtime:sleep(2);
    string[] apartmentCodes = check cacheClient->/apartments.post([apartment2]);
    test:assertEquals(apartmentCodes, [apartment2.code]);
    runtime:sleep(1);
    Person personRetrieved = check cacheClient->/people/[person1.id];
    test:assertEquals(personRetrieved, person1);
    apartmentCodes = check cacheClient->/apartments.post([apartment1]);
    test:assertEquals(apartmentCodes, [apartment1.code]);
    runtime:sleep(3);

    PersonWithAssoc person = check cacheClient->/people/[person1.id];
    test:assertEquals(person, person1WithAssoc);

    runtime:sleep(4);
    check cacheClient.close();
}

@test:Config {
    groups: ["cache", "redis"],
    dependsOn: [redisEmployeeAndSetTest],
    enable: false
}
function redisMiscTest() returns error? {
    RedisCacheClient cacheClient = check new ();

    int[] ids = check cacheClient->/people.post([person1]);
    test:assertEquals(ids, [person1.id]);
    int[] ids2 = check cacheClient->/people.post([person2]);
    test:assertEquals(ids2, [person2.id]);

    string[] apartmentCodes = check cacheClient->/apartments.post([apartment1]);
    test:assertEquals(apartmentCodes, [apartment1.code]);

    stream<PersonWithAssoc, persist:Error?> peopleStream = cacheClient->/people;
    PersonWithAssoc[] people = check from PersonWithAssoc person2 in peopleStream
        order by person2.id ascending
        select person2;
    test:assertEquals(people, peopleWithAssoc);

    stream<ApartmentWithAssoc, persist:Error?> apartmentsStream = cacheClient->/apartments;
    ApartmentWithAssoc[] apartments = check from ApartmentWithAssoc apartment2 in apartmentsStream
        order by apartment2.code ascending
        select apartment2;
    test:assertEquals(apartments, apartmentsWithAssoc);

    _ = check cacheClient->/apartments/[apartment1.code].delete();
    Apartment|persist:Error apartment1Retreived = cacheClient->/apartments/[apartment1.code];
    test:assertTrue(apartment1Retreived is persist:NotFoundError, "Should return a `persist:NotFoundError`");
    PersonWithAssoc|persist:Error person1WithAssoc = cacheClient->/people/[person1.id];
    test:assertEquals(person1WithAssoc, person1WithoutAssoc);

    _ = check cacheClient->/people/[person1.id].delete();
    Person|persist:Error person1Retreived = cacheClient->/people/[person1.id];
    test:assertTrue(person1Retreived is persist:NotFoundError, "Should return a `persist:NotFoundError`");

    runtime:sleep(4);
    check cacheClient.close();
}

type PersonWithAssoc record {|
    readonly int id;
    string name;
    record {|
        string code;
    |}[] soldBuildings;
|};

type ApartmentWithAssoc record {|
    string code;
    string city;
    string state;
    record {|
        int id;
        string name;
    |} soldPerson;
|};
