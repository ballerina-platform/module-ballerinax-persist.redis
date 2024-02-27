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
    groups: ["workspace", "redis"],
    dependsOn: [redisBuildingDeleteTestNegative]
}
function redisWorkspaceCreateTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    string[] workspaceIds = check rainierClient->/workspaces.post([workspace1]);
    test:assertEquals(workspaceIds, [workspace1.workspaceId]);

    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId];
    test:assertEquals(workspaceRetrieved, workspace1);
}

@test:Config {
    groups: ["workspace", "redis"]
}
function redisWorkspaceCreateTest2() returns error? {
    RedisRainierClient rainierClient = check new ();

    string[] workspaceIds = check rainierClient->/workspaces.post([workspace2, workspace3]);

    test:assertEquals(workspaceIds, [workspace2.workspaceId, workspace3.workspaceId]);

    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace2.workspaceId];
    test:assertEquals(workspaceRetrieved, workspace2);

    workspaceRetrieved = check rainierClient->/workspaces/[workspace3.workspaceId];
    test:assertEquals(workspaceRetrieved, workspace3);
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceCreateTest]
}
function redisWorkspaceReadOneTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId];
    test:assertEquals(workspaceRetrieved, workspace1);
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceCreateTest]
}
function redisWorkspaceReadOneDependentTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    WorkspaceInfo2 workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId];
    test:assertEquals(workspaceRetrieved,
        {
        workspaceType: workspace1.workspaceType,
        locationBuildingCode: workspace1.locationBuildingCode
    }
    );
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceCreateTest]
}
function redisWorkspaceReadOneTestNegative() returns error? {
    RedisRainierClient rainierClient = check new ();

    Workspace|error workspaceRetrieved = rainierClient->/workspaces/["invalid-workspace-id"];
    if workspaceRetrieved is persist:NotFoundError {
        test:assertEquals(workspaceRetrieved.message(), "A record with the key 'Workspace:invalid-workspace-id' does not exist for the entity 'Workspace'.");
    } else {
        test:assertFail("NotFoundError expected.");
    }
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceCreateTest, redisWorkspaceCreateTest2]
}
function redisWorkspaceReadManyTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    stream<Workspace, error?> workspaceStream = rainierClient->/workspaces;
    Workspace[] workspaces = check from Workspace workspace in workspaceStream
        order by workspace.workspaceId ascending select workspace;

    test:assertEquals(workspaces, [workspace1, workspace2, workspace3]);
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis", "dependent"],
    dependsOn: [redisWorkspaceCreateTest, redisWorkspaceCreateTest2]
}
function redisWorkspaceReadManyDependentTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    stream<WorkspaceInfo2, error?> workspaceStream = rainierClient->/workspaces;
    WorkspaceInfo2[] workspaces = check from WorkspaceInfo2 workspace in workspaceStream
        order by workspace.workspaceType select workspace;

    test:assertEquals(workspaces, [
        {workspaceType: workspace3.workspaceType, locationBuildingCode: workspace3.locationBuildingCode},
        {workspaceType: workspace2.workspaceType, locationBuildingCode: workspace2.locationBuildingCode},
        {workspaceType: workspace1.workspaceType, locationBuildingCode: workspace1.locationBuildingCode}
    ]);
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceReadOneTest, redisWorkspaceReadManyTest, redisWorkspaceReadManyDependentTest]
}
function redisWorkspaceUpdateTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    Workspace workspace = check rainierClient->/workspaces/[workspace1.workspaceId].put({
        workspaceType: "large"
    });

    test:assertEquals(workspace, updatedWorkspace1);

    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId];
    test:assertEquals(workspaceRetrieved, updatedWorkspace1);
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceReadOneTest, redisWorkspaceReadManyTest, redisWorkspaceReadManyDependentTest]
}
function redisWorkspaceUpdateTestNegative1() returns error? {
    RedisRainierClient rainierClient = check new ();

    Workspace|error workspace = rainierClient->/workspaces/["invalid-workspace-id"].put({
        workspaceType: "large"
    });

    if workspace is persist:NotFoundError {
        test:assertEquals(workspace.message(), "A record with the key 'Workspace:invalid-workspace-id' does not exist for the entity 'Workspace'.");
    } else {
        test:assertFail("NotFoundError expected.");
    }
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceUpdateTest]
}
function redisWorkspaceDeleteTest() returns error? {
    RedisRainierClient rainierClient = check new ();

    Workspace workspace = check rainierClient->/workspaces/[workspace1.workspaceId].delete();
    test:assertEquals(workspace, updatedWorkspace1);

    stream<Workspace, error?> workspaceStream = rainierClient->/workspaces;
    Workspace[] workspaces = check from Workspace workspace2 in workspaceStream
        order by workspace2.workspaceId ascending select workspace2;

    test:assertEquals(workspaces, [workspace2, workspace3]);
    check rainierClient.close();
}

@test:Config {
    groups: ["workspace", "redis"],
    dependsOn: [redisWorkspaceDeleteTest]
}
function redisWorkspaceDeleteTestNegative() returns error? {
    RedisRainierClient rainierClient = check new ();

    Workspace|error workspace = rainierClient->/workspaces/[workspace1.workspaceId].delete();

    if workspace is persist:NotFoundError {
        test:assertEquals(workspace.message(), string `A record with the key 'Workspace:${workspace1.workspaceId}' does not exist for the entity 'Workspace'.`);
    } else {
        test:assertFail("NotFoundError expected.");
    }
    check rainierClient.close();
}
