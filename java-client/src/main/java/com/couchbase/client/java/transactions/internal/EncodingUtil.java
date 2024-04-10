/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions.internal;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.msg.kv.CodecFlags.JSON_COMMON_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.JSON_COMPAT_FLAGS;

@Stability.Internal
public class EncodingUtil {
    private EncodingUtil() {
    }

    public static Transcoder.EncodedValue encode(Object content, RequestSpan span, JsonSerializer serializer, @Nullable Transcoder transcoder, CoreContext coreContext) {
        RequestSpan encoding = CbTracing.newSpan(coreContext, TracingIdentifiers.SPAN_REQUEST_ENCODING, span);

        Transcoder.EncodedValue encoded;
        try {
            if (transcoder != null) {
                encoded = transcoder.encode(content);
            }
            else {
                byte[] bytes = serializer.serialize(content);
                encoded = new Transcoder.EncodedValue(bytes, JSON_COMPAT_FLAGS);
            }
            encoding.end();
        } catch (Throwable err) {
            encoding.recordException(err);
            encoding.status(RequestSpan.StatusCode.ERROR);
            throw err;
        }

        return encoded;
    }

}
