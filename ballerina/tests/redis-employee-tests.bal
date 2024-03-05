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
import ballerina/test;

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisWorkspaceDeleteTestNegative, redisDepartmentDeleteTestNegative]
}
function redisEmployeeCreateTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    string[] empNos = check rainierClient->/employees.post([employee1]);
    test:assertEquals(empNos, [employee1.empNo]);

    Employee employeeRetrieved = check rainierClient->/employees/[employee1.empNo];
    test:assertEquals(employeeRetrieved, employee1);
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisWorkspaceDeleteTestNegative, redisDepartmentDeleteTestNegative]
}
function redisEmployeeCreateTest2() returns error? {
    RedisRainierClient rainierClient = check new ();

    string[] empNos = check rainierClient->/employees.post([employee2, employee3]);

    test:assertEquals(empNos, [employee2.empNo, employee3.empNo]);

    Employee employeeRetrieved = check rainierClient->/employees/[employee2.empNo];
    test:assertEquals(employeeRetrieved, employee2);

    employeeRetrieved = check rainierClient->/employees/[employee3.empNo];
    test:assertEquals(employeeRetrieved, employee3);
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeCreateTest]
}
function redisEmployeeReadOneTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    Employee employeeRetrieved = check rainierClient->/employees/[employee1.empNo];
    test:assertEquals(employeeRetrieved, employee1);
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeCreateTest]
}
function redisEmployeeReadOneTestNegative() returns error? {
    RedisRainierClient rainierClient = check new ();

    Employee|error employeeRetrieved = rainierClient->/employees/["invalid-employee-id"];
    if employeeRetrieved is persist:NotFoundError {
        test:assertEquals(employeeRetrieved.message(), 
        "A record with the key 'Employee:invalid-employee-id' does not exist for the entity 'Employee'.");
    } else {
        test:assertFail("NotFoundError expected.");
    }
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeCreateTest, redisEmployeeCreateTest2]
}
function redisEmployeeReadManyTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    stream<Employee, persist:Error?> employeeStream = rainierClient->/employees;
    Employee[] employees = check from Employee employee in employeeStream
        order by employee.empNo ascending select employee;

    test:assertEquals(employees, [employee1, employee2, employee3]);
    check rainierClient.close();
}

@test:Config {
    groups: ["dependent", "employee"],
    dependsOn: [redisEmployeeCreateTest, redisEmployeeCreateTest2]
}
function redisEmployeeReadManyDependentTest1() returns error? {
    RedisRainierClient rainierClient = check new ();

    stream<EmployeeName, persist:Error?> employeeStream = rainierClient->/employees;
    EmployeeName[] employees = check from EmployeeName employee in employeeStream
        order by employee.firstName ascending select employee;

    test:assertEquals(employees, [
        {firstName: employee3.firstName, lastName: employee3.lastName},
        {firstName: employee2.firstName, lastName: employee2.lastName},
        {firstName: employee1.firstName, lastName: employee1.lastName}
    ]);
    check rainierClient.close();
}

@test:Config {
    groups: ["dependent", "employee"],
    dependsOn: [redisEmployeeCreateTest, redisEmployeeCreateTest2]
}
function redisEmployeeReadManyDependentTest2() returns error? {
    RedisRainierClient rainierClient = check new ();

    stream<EmployeeInfo2, persist:Error?> employeeStream = rainierClient->/employees;
    EmployeeInfo2[] employees = check from EmployeeInfo2 employee in employeeStream
        order by employee.empNo ascending select employee;

    test:assertEquals(employees, [
        {empNo: employee1.empNo, birthDate: employee1.birthDate, departmentDeptNo: employee1.departmentDeptNo, 
        workspaceWorkspaceId: employee1.workspaceWorkspaceId},
        {empNo: employee2.empNo, birthDate: employee2.birthDate, departmentDeptNo: employee2.departmentDeptNo, 
        workspaceWorkspaceId: employee2.workspaceWorkspaceId},
        {empNo: employee3.empNo, birthDate: employee3.birthDate, departmentDeptNo: employee3.departmentDeptNo, 
        workspaceWorkspaceId: employee3.workspaceWorkspaceId}
    ]);
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeReadOneTest, redisEmployeeReadManyTest, redisEmployeeReadManyDependentTest1, 
    redisEmployeeReadManyDependentTest2]
}
function redisEmployeeUpdateTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    Employee employee = check rainierClient->/employees/[employee1.empNo].put({
        lastName: updatedEmployee1.lastName,
        departmentDeptNo: updatedEmployee1.departmentDeptNo,
        birthDate: updatedEmployee1.birthDate
    });

    test:assertEquals(employee, updatedEmployee1);

    Employee employeeRetrieved = check rainierClient->/employees/[employee1.empNo];
    test:assertEquals(employeeRetrieved, updatedEmployee1);
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeReadOneTest, redisEmployeeReadManyTest, redisEmployeeReadManyDependentTest1, 
    redisEmployeeReadManyDependentTest2]
}
function redisEmployeeUpdateTestNegative1() returns error? {
    RedisRainierClient rainierClient = check new ();

    Employee|error employee = rainierClient->/employees/["invalid-employee-id"].put({
        lastName: "Jones"
    });

    if employee is persist:NotFoundError {
        test:assertEquals(employee.message(), 
        "A record with the key 'Employee:invalid-employee-id' does not exist for the entity 'Employee'.");
    } else {
        test:assertFail("NotFoundError expected.");
    }
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeReadOneTest, redisEmployeeReadManyTest, redisEmployeeReadManyDependentTest1, 
    redisEmployeeReadManyDependentTest2]
}
function redisEmployeeUpdateTestNegative3() returns error? {
    RedisRainierClient rainierClient = check new ();

    Employee|error employee = rainierClient->/employees/[employee1.empNo].put({
        workspaceWorkspaceId: "invalid-workspaceWorkspaceId"
    });

    if employee is persist:ConstraintViolationError {
        test:assertTrue(employee.message().includes(
            string `An association constraint failed between entities 'Employee' and 'Workspace'`));
    } else {
        test:assertFail("persist:ConstraintViolationError expected.");
    }
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeUpdateTest, redisEmployeeUpdateTestNegative3]
}
function redisEmployeeDeleteTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    Employee employee = check rainierClient->/employees/[employee1.empNo].delete();
    test:assertEquals(employee, updatedEmployee1);

    stream<Employee, error?> employeeStream = rainierClient->/employees;
    Employee[] employees = check from Employee employee2 in employeeStream
        order by employee2.empNo ascending select employee2;

    test:assertEquals(employees, [employee2, employee3]);
    check rainierClient.close();
}

@test:Config {
    groups: ["employee", "redis"],
    dependsOn: [redisEmployeeDeleteTest]
}
function redisEmployeeDeleteTestNegative() returns error? {
    RedisRainierClient rainierClient = check new ();

    Employee|error employee = rainierClient->/employees/[employee1.empNo].delete();

    if employee is persist:NotFoundError {
        test:assertEquals(employee.message(), 
        string `A record with the key 'Employee:${employee1.empNo}' does not exist for the entity 'Employee'.`);
    } else {
        test:assertFail("NotFoundError expected.");
    }
    check rainierClient.close();
}
