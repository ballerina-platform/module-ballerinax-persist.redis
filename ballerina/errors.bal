import ballerina/persist;


public type ConstraintViolationError distinct persist:ConstraintViolationError;

# Generates a new `persist:AlreadyExistsError` with the given parameters.
#
# + entity - The name of the entity  
# + refEntity - The entity is being reffered
# + return - The generated `persist:ConstraintViolationError`
public isolated function getConstraintViolationError(string entity, string refEntity) returns ConstraintViolationError {
    string message = string `A relationship constrant failed between entities '${entity}' and '${refEntity}'`;
    return error ConstraintViolationError(message);
}