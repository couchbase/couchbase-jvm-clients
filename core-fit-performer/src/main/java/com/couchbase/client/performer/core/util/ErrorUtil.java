/*
 * Copyright (c) 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.performer.core.util;

import com.couchbase.client.protocol.shared.CouchbaseExceptionType;
import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

import static com.couchbase.client.protocol.shared.CouchbaseExceptionType.SDK_DURABLE_WRITE_RECOMMIT_IN_PROGRESS_EXCEPTION;
import static com.couchbase.client.protocol.shared.CouchbaseExceptionType.SDK_PATH_TOO_BIG_EXCEPTION;
import static com.couchbase.client.protocol.shared.CouchbaseExceptionType.SDK_PATH_TOO_DEEP_EXCEPTION;
import static com.couchbase.client.protocol.shared.CouchbaseExceptionType.SDK_REQUEST_CANCELLED_EXCEPTION;

public class ErrorUtil {
    private static final Logger logger = LoggerFactory.getLogger(ErrorUtil.class);

    private ErrorUtil() {
        throw new AssertionError("not instantiable");
    }

    /**
     * Converts "FooBar" to "FOO_BAR", for example.
     */
    private static final Converter<String, String> upperCamelToUpperUnderscore =
            CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.UPPER_UNDERSCORE);

    /**
     * A "fix-up" map for naughty exceptions that don't follow the naming convention.
     */
    private static final Map<String, CouchbaseExceptionType> irregular = Map.of(
            "DurableWriteReCommitInProgressException", SDK_DURABLE_WRITE_RECOMMIT_IN_PROGRESS_EXCEPTION,
            "RequestCanceledException", SDK_REQUEST_CANCELLED_EXCEPTION,
            "PathTooDeepException", SDK_PATH_TOO_BIG_EXCEPTION,
            "DocumentTooDeepException", SDK_PATH_TOO_DEEP_EXCEPTION
    );

    public static @Nullable CouchbaseExceptionType convertException(String simpleClassName) {
        CouchbaseExceptionType type = irregular.get(simpleClassName);
        if (type != null) return type;

        // Convert "MyCouchbaseException" to "SDK_MY_COUCHBASE_EXCEPTION", for example.
        String enumValueName = "SDK_" + upperCamelToUpperUnderscore.convert(simpleClassName);
        try {
            return CouchbaseExceptionType.valueOf(enumValueName);

        } catch (IllegalArgumentException e) {
            logger.warn("Failed to convert {} to {}.{} (not a valid enum value)",
                    simpleClassName, CouchbaseExceptionType.class.getSimpleName(), enumValueName);
            // We should only be here if the original exception derives from CouchbaseException.
            // (But we can't assert that here without adding a dependency on core-io).
            // There are some errors like HttpStatusCodeException which do, but aren't in the FIT enum as they're not specced.
            return CouchbaseExceptionType.SDK_COUCHBASE_EXCEPTION;
        }
    }

    public static @Nullable CouchbaseExceptionType convertException(Throwable err) {
        // We do string comparisons to avoid core-fit-performer needing to depend on core-io, which introduced a world
        // of Maven versioning hurt when trying to build the performers against specific SDK versions.
        return convertException(err.getClass().getSimpleName());
    }
}
