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
import ballerina/jballerina.java;
import ballerinax/redis;

const EMPLOYEE = "employees";
const WORKSPACE = "workspaces";
const BUILDING = "buildings";
const DEPARTMENT = "departments";
const ORDER_ITEM = "orderitems";

public isolated client class RedisRainierClient {
    *persist:AbstractPersistClient;

    private final redis:Client dbClient;

    private final map<RedisClient> persistClients;

    private final record {|RedisMetadata...;|} & readonly metadata = {
        [EMPLOYEE] : {
            entityName: "Employee",
            collectionName: "Employee",
            fieldMetadata: {
                empNo: {fieldName: "empNo", fieldDataType: STRING},
                firstName: {fieldName: "firstName", fieldDataType: STRING},
                lastName: {fieldName: "lastName", fieldDataType: STRING},
                birthDate: {fieldName: "birthDate", fieldDataType: DATE},
                gender: {fieldName: "gender", fieldDataType: ENUM},
                hireDate: {fieldName: "hireDate", fieldDataType: DATE},
                departmentDeptNo: {fieldName: "departmentDeptNo", fieldDataType: STRING},
                workspaceWorkspaceId: {fieldName: "workspaceWorkspaceId", fieldDataType: STRING},
                "department.deptNo": {relation: {entityName: "department", refField: "deptNo", refFieldDataType: STRING}},
                "department.deptName": {relation: {entityName: "department", refField: "deptName", refFieldDataType: STRING}},
                "workspace.workspaceId": {relation: {entityName: "workspace", refField: "workspaceId", refFieldDataType: STRING}},
                "workspace.workspaceType": {relation: {entityName: "workspace", refField: "workspaceType", refFieldDataType: STRING}},
                "workspace.locationBuildingCode": {relation: {entityName: "workspace", refField: "locationBuildingCode", refFieldDataType: STRING}}
            },
            keyFields: ["empNo"],
            refMetadata: {
                department: {entity: Department, fieldName: "department", refCollection: "Department", refFields: ["deptNo"], joinFields: ["departmentDeptNo"], 'type: ONE_TO_MANY},
                workspace: {entity: Workspace, fieldName: "workspace", refCollection: "Workspace", refFields: ["workspaceId"], joinFields: ["workspaceWorkspaceId"], 'type: ONE_TO_MANY}
            }
        },
        [WORKSPACE] : {
            entityName: "Workspace",
            collectionName: "Workspace",
            fieldMetadata: {
                workspaceId: {fieldName: "workspaceId", fieldDataType: STRING},
                workspaceType: {fieldName: "workspaceType", fieldDataType: STRING},
                locationBuildingCode: {fieldName: "locationBuildingCode", fieldDataType: STRING},
                "location.buildingCode": {relation: {entityName: "location", refField: "buildingCode", refFieldDataType: STRING}},
                "location.city": {relation: {entityName: "location", refField: "city", refFieldDataType: STRING}},
                "location.state": {relation: {entityName: "location", refField: "state", refFieldDataType: STRING}},
                "location.country": {relation: {entityName: "location", refField: "country", refFieldDataType: STRING}},
                "location.postalCode": {relation: {entityName: "location", refField: "postalCode", refFieldDataType: STRING}},
                "location.type": {relation: {entityName: "location", refField: "type", refFieldDataType: STRING}},
                "employees[].empNo": {relation: {entityName: "employees", refField: "empNo", refFieldDataType: STRING}},
                "employees[].firstName": {relation: {entityName: "employees", refField: "firstName", refFieldDataType: STRING}},
                "employees[].lastName": {relation: {entityName: "employees", refField: "lastName", refFieldDataType: STRING}},
                "employees[].birthDate": {relation: {entityName: "employees", refField: "birthDate", refFieldDataType: DATE}},
                "employees[].gender": {relation: {entityName: "employees", refField: "gender", refFieldDataType: ENUM}},
                "employees[].hireDate": {relation: {entityName: "employees", refField: "hireDate", refFieldDataType: DATE}},
                "employees[].departmentDeptNo": {relation: {entityName: "employees", refField: "departmentDeptNo", refFieldDataType: STRING}},
                "employees[].workspaceWorkspaceId": {relation: {entityName: "employees", refField: "workspaceWorkspaceId", refFieldDataType: STRING}}
            },
            keyFields: ["workspaceId"],
            refMetadata: {
                location: {entity: Building, fieldName: "location", refCollection: "Building", refFields: ["buildingCode"], joinFields: ["locationBuildingCode"], 'type: ONE_TO_MANY},
                employees: {entity: Employee, fieldName: "employees", refCollection: "Employee", refFields: ["workspaceWorkspaceId"], joinFields: ["workspaceId"], 'type: MANY_TO_ONE}
            }
        },
        [BUILDING] : {
            entityName: "Building",
            collectionName: "Building",
            fieldMetadata: {
                buildingCode: {fieldName: "buildingCode", fieldDataType: STRING},
                city: {fieldName: "city", fieldDataType: STRING},
                state: {fieldName: "state", fieldDataType: STRING},
                country: {fieldName: "country", fieldDataType: STRING},
                postalCode: {fieldName: "postalCode", fieldDataType: STRING},
                'type: {fieldName: "type", fieldDataType: STRING},
                "workspaces[].workspaceId": {relation: {entityName: "workspaces", refField: "workspaceId", refFieldDataType: STRING}},
                "workspaces[].workspaceType": {relation: {entityName: "workspaces", refField: "workspaceType", refFieldDataType: STRING}},
                "workspaces[].locationBuildingCode": {relation: {entityName: "workspaces", refField: "locationBuildingCode", refFieldDataType: STRING}}
            },
            keyFields: ["buildingCode"],
            refMetadata: {workspaces: {entity: Workspace, fieldName: "workspaces", refCollection: "Workspace", refFields: ["locationBuildingCode"], joinFields: ["buildingCode"], 'type: MANY_TO_ONE}}
        },
        [DEPARTMENT] : {
            entityName: "Department",
            collectionName: "Department",
            fieldMetadata: {
                deptNo: {fieldName: "deptNo", fieldDataType: STRING},
                deptName: {fieldName: "deptName", fieldDataType: STRING},
                "employees[].empNo": {relation: {entityName: "employees", refField: "empNo", refFieldDataType: STRING}},
                "employees[].firstName": {relation: {entityName: "employees", refField: "firstName", refFieldDataType: STRING}},
                "employees[].lastName": {relation: {entityName: "employees", refField: "lastName", refFieldDataType: STRING}},
                "employees[].birthDate": {relation: {entityName: "employees", refField: "birthDate", refFieldDataType: DATE}},
                "employees[].gender": {relation: {entityName: "employees", refField: "gender", refFieldDataType: ENUM}},
                "employees[].hireDate": {relation: {entityName: "employees", refField: "hireDate", refFieldDataType: DATE}},
                "employees[].departmentDeptNo": {relation: {entityName: "employees", refField: "departmentDeptNo", refFieldDataType: STRING}},
                "employees[].workspaceWorkspaceId": {relation: {entityName: "employees", refField: "workspaceWorkspaceId", refFieldDataType: STRING}}
            },
            keyFields: ["deptNo"],
            refMetadata: {employees: {entity: Employee, fieldName: "employees", refCollection: "Employee", refFields: ["departmentDeptNo"], joinFields: ["deptNo"], 'type: MANY_TO_ONE}}
        },
        [ORDER_ITEM] : {
            entityName: "OrderItem",
            collectionName: "OrderItem",
            fieldMetadata: {
                orderId: {fieldName: "orderId", fieldDataType: STRING},
                itemId: {fieldName: "itemId", fieldDataType: STRING},
                quantity: {fieldName: "quantity", fieldDataType: INT},
                notes: {fieldName: "notes", fieldDataType: STRING}
            },
            keyFields: ["orderId", "itemId"]
        }
    };

    public isolated function init() returns persist:Error? {
        redis:Client|error dbClient = new (config = { host: redis.host+":"+redis.port.toString(), password: redis.password, options: redis.connectionOptions });
        if dbClient is error {
            return <persist:Error>error(dbClient.message());
        }
        self.dbClient = dbClient;
        self.persistClients = {
            [EMPLOYEE] : check new (dbClient, self.metadata.get(EMPLOYEE)),
            [WORKSPACE] : check new (dbClient, self.metadata.get(WORKSPACE)),
            [BUILDING] : check new (dbClient, self.metadata.get(BUILDING)),
            [DEPARTMENT] : check new (dbClient, self.metadata.get(DEPARTMENT)),
            [ORDER_ITEM] : check new (dbClient, self.metadata.get(ORDER_ITEM))
        };
    }

    isolated resource function get employees(EmployeeTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get employees/[string empNo](EmployeeTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post employees(EmployeeInsert[] data) returns string[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(EMPLOYEE);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from EmployeeInsert inserted in data
            select inserted.empNo;
    }

    isolated resource function put employees/[string empNo](EmployeeUpdate value) returns Employee|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(EMPLOYEE);
        }
        _ = check redisClient.runUpdateQuery(empNo, value);
        return self->/employees/[empNo];
    }

    isolated resource function delete employees/[string empNo]() returns Employee|persist:Error {
        Employee result = check self->/employees/[empNo];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(EMPLOYEE);
        }
        _ = check redisClient.runDeleteQuery(empNo);
        return result;
    }

    isolated resource function get workspaces(WorkspaceTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get workspaces/[string workspaceId](WorkspaceTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post workspaces(WorkspaceInsert[] data) returns string[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(WORKSPACE);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from WorkspaceInsert inserted in data
            select inserted.workspaceId;
    }

    isolated resource function put workspaces/[string workspaceId](WorkspaceUpdate value) returns Workspace|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(WORKSPACE);
        }
        _ = check redisClient.runUpdateQuery(workspaceId, value);
        return self->/workspaces/[workspaceId];
    }

    isolated resource function delete workspaces/[string workspaceId]() returns Workspace|persist:Error {
        Workspace result = check self->/workspaces/[workspaceId];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(WORKSPACE);
        }
        _ = check redisClient.runDeleteQuery(workspaceId);
        return result;
    }

    isolated resource function get buildings(BuildingTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get buildings/[string buildingCode](BuildingTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post buildings(BuildingInsert[] data) returns string[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(BUILDING);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from BuildingInsert inserted in data
            select inserted.buildingCode;
    }

    isolated resource function put buildings/[string buildingCode](BuildingUpdate value) returns Building|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(BUILDING);
        }
        _ = check redisClient.runUpdateQuery(buildingCode, value);
        return self->/buildings/[buildingCode];
    }

    isolated resource function delete buildings/[string buildingCode]() returns Building|persist:Error {
        Building result = check self->/buildings/[buildingCode];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(BUILDING);
        }
        _ = check redisClient.runDeleteQuery(buildingCode);
        return result;
    }

    isolated resource function get departments(DepartmentTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get departments/[string deptNo](DepartmentTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post departments(DepartmentInsert[] data) returns string[]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(DEPARTMENT);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from DepartmentInsert inserted in data
            select inserted.deptNo;
    }

    isolated resource function put departments/[string deptNo](DepartmentUpdate value) returns Department|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(DEPARTMENT);
        }
        _ = check redisClient.runUpdateQuery(deptNo, value);
        return self->/departments/[deptNo];
    }

    isolated resource function delete departments/[string deptNo]() returns Department|persist:Error {
        Department result = check self->/departments/[deptNo];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(DEPARTMENT);
        }
        _ = check redisClient.runDeleteQuery(deptNo);
        return result;
    }

    isolated resource function get orderitems(OrderItemTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "query"
    } external;

    isolated resource function get orderitems/[string orderId]/[string itemId](OrderItemTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.redis.datastore.RedisProcessor",
        name: "queryOne"
    } external;

    isolated resource function post orderitems(OrderItemInsert[] data) returns [string, string][]|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ORDER_ITEM);
        }
        _ = check redisClient.runBatchInsertQuery(data);
        return from OrderItemInsert inserted in data
            select [inserted.orderId, inserted.itemId];
    }

    isolated resource function put orderitems/[string orderId]/[string itemId](OrderItemUpdate value) returns OrderItem|persist:Error {
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ORDER_ITEM);
        }
        _ = check redisClient.runUpdateQuery({"orderId": orderId, "itemId": itemId}, value);
        return self->/orderitems/[orderId]/[itemId];
    }

    isolated resource function delete orderitems/[string orderId]/[string itemId]() returns OrderItem|persist:Error {
        OrderItem result = check self->/orderitems/[orderId]/[itemId];
        RedisClient redisClient;
        lock {
            redisClient = self.persistClients.get(ORDER_ITEM);
        }
        _ = check redisClient.runDeleteQuery({"orderId": orderId, "itemId": itemId});
        return result;
    }

    public isolated function close() returns persist:Error? {
        error? result = self.dbClient.stop();
        if result is error {
            return <persist:Error>error(result.message());
        }
        return result;
    }
}
