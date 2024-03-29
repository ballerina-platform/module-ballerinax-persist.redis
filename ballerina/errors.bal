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

# Generates a new `persist:ConstraintViolationError` with the given parameters.
#
# + entity - The name of the entity  
# + refEntity - The entity is being reffered
# + return - The generated `persist:ConstraintViolationError`
public isolated function getConstraintViolationError(string entity, string refEntity) 
returns persist:ConstraintViolationError {
    string message = string `An association constraint failed between entities '${entity}' and '${refEntity}'`;
    return error persist:ConstraintViolationError(message);
}


# Generates a new `persist:AlreadyExistsError` with the given parameters.
#
# + entity - The name of the entity
# + keyCount - The number of keys already exists
# + return - The generated `persist:AlreadyExistsError`
public isolated function getAlreadyExistsError(string entity, int keyCount) returns persist:AlreadyExistsError {
    return error persist:AlreadyExistsError(
        string `Record(s) already exist with the same key for the entity '${entity}'. Number of keys exists : ${keyCount}`);
}
