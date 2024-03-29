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
import ballerina/time;

# Generic type that can used to store any of the types supported by Redis
#
public type RedisFieldType string|int|decimal|boolean|float|time:Date|time:TimeOfDay|time:Civil|time:Utc;

# Generic type that can used to store any of time types supported by Redis
#
public type RedisTimeType time:Date|time:TimeOfDay|time:Civil|time:Utc;

# Generic type that can used to store any of basic numeric types supported by Redis
#
public type RedisNumericType int|decimal|boolean|float;

# Generic type that can used to store any of basic types supported by Redis
#
public type RedisBasicType int|string|decimal|boolean|float;
