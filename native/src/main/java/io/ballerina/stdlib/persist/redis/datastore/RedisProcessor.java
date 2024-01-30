
package io.ballerina.stdlib.persist.redis.datastore;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Future;
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.async.Callback;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.persist.Constants;
import io.ballerina.stdlib.persist.redis.Utils;

import java.util.Map;

import static io.ballerina.stdlib.persist.Constants.KEY_FIELDS;
import static io.ballerina.stdlib.persist.ErrorGenerator.wrapError;
import static io.ballerina.stdlib.persist.Utils.getEntity;
import static io.ballerina.stdlib.persist.Utils.getMetadata;
import static io.ballerina.stdlib.persist.Utils.getPersistClient;
import static io.ballerina.stdlib.persist.Utils.getRecordTypeWithKeyFields;
import static io.ballerina.stdlib.persist.Utils.getTransactionContextProperties;
import static io.ballerina.stdlib.persist.redis.Utils.getFieldTypes;

public class RedisProcessor {

    private RedisProcessor() {

    }
    
    public static BStream query(Environment env, BObject client, BTypedesc targetType) {
        // This method will return `stream<targetType, persist:Error?>`

        BString entity = getEntity(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        StreamType streamTypeWithIdFields = TypeCreator.createStreamType(recordTypeWithIdFields,
                PredefinedTypes.TYPE_NULL);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        String strandName = env.getStrandName().isPresent() ? env.getStrandName().get() : null;

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];
        BMap<BString, Object> typeMap = getFieldTypes(recordType);

        Future balFuture = env.markAsync();
        env.getRuntime().invokeMethodAsyncSequentially(
                // Call `RedisClient.runReadQuery(
                //      map<anydata> typeMap, string[] fields = [], string[] include = []
                // )`
                // which returns `stream<record{}|error?>|persist:Error`

                persistClient, Constants.RUN_READ_QUERY_METHOD, strandName, env.getStrandMetadata(), new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        if (o instanceof BStream) { // stream<record {}, redis:Error?>
                            BStream redisStream = (BStream) o;
                            balFuture.complete(Utils.createPersistRedisStreamValue(redisStream, targetType, fields,
                                    includes, typeDescriptions, persistClient, null));
                        } else { // persist:Error
                            balFuture.complete(Utils.createPersistRedisStreamValue(null, targetType, fields, includes,
                                    typeDescriptions, persistClient, (BError) o));
                        }
                    }

                    @Override
                    public void notifyFailure(BError bError) {
                        balFuture.complete(Utils.createPersistRedisStreamValue(null, targetType, fields, includes,
                                typeDescriptions, persistClient, wrapError(bError)));
                    }
                }, trxContextProperties, streamTypeWithIdFields,
                typeMap, true, fields, true, includes, true
        );

        return null;
    }

}
