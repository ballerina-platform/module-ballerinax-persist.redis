
package io.ballerina.stdlib.persist.redis.datastore;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Future;
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.async.Callback;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
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

public class RedisProcessor {

    private RedisProcessor() {

    }
    
    static BStream query(Environment env, BObject client, BTypedesc targetType) {
        // This method will return `stream<targetType, persist:Error?>`

        BString entity = getEntity(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        BTypedesc targetTypeWithIdFields = ValueCreator.createTypedescValue(recordTypeWithIdFields);
        StreamType streamTypeWithIdFields = TypeCreator.createStreamType(recordTypeWithIdFields,
                PredefinedTypes.TYPE_NULL);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        String strandName = env.getStrandName().isPresent() ? env.getStrandName().get() : null;

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];

        Future balFuture = env.markAsync();
        env.getRuntime().invokeMethodAsyncSequentially(
                // Call `SQLClient.runReadQuery(
                //      typedesc<record {}> rowType, string[] fields = [], string[] include = []
                // )`
                // which returns `stream<record {}, sql:Error?>|persist:Error`

                persistClient, Constants.RUN_READ_QUERY_METHOD, strandName, env.getStrandMetadata(), new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        if (o instanceof BStream) { // stream<record {}, sql:Error?>
                            BStream sqlStream = (BStream) o;
                            balFuture.complete(Utils.createPersistRedisStreamValue(sqlStream, targetType, fields,
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
                targetTypeWithIdFields, true, fields, true, includes, true
        );

        return null;
    }

}
