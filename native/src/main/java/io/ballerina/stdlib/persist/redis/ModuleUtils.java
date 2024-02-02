package io.ballerina.stdlib.persist.redis;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Module;

public class ModuleUtils {
    private static Module redisModule;

    private ModuleUtils() {
    }

    public static void setModule(Environment env) {
        redisModule = env.getCurrentModule();
    }

    public static Module getModule() {
        return redisModule;
    }
}
