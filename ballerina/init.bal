import ballerina/jballerina.java;

isolated function init() {
    setModule();
}

isolated function setModule() = @java:Method {
    'class: "io.ballerina.stdlib.persist.redis.ModuleUtils"
} external;