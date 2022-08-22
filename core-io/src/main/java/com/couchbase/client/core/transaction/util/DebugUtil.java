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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import reactor.util.annotation.Nullable;

/*
 * These return LogDefer* objects rather that Strings, so log evaluation only has to happen if the logs are actually
 * written somewhere.
 */
@Stability.Internal
public class DebugUtil {
    private DebugUtil() { }

    public static LogDeferThrowable dbg(Throwable err) {
        if (err == null) {
            return null;
        }
        return new LogDeferThrowable(err);
    }

    public static LogDeferDocId docId(CoreTransactionGetResult doc) {
        return new LogDeferDocId(doc.collection(), doc.id());
    }

    public static LogDeferDocId docId(CollectionIdentifier collection, String docId) {
        return new LogDeferDocId(collection, docId);
    }

    // Printing the stacktrace is expensive in terms of log noise, but has been a life saver on many debugging
    // encounters.  Strike a balance by eliding the more useless elements.
    // This version captures it all on one line.  It's less readable, but the atomicity is helpful when debugging concurrent ops.
    public static String createElidedStacktrace(Throwable err) {
        StringBuilder sb = new StringBuilder();
        StackTraceElement[] st = err.getStackTrace();
        for (StackTraceElement s : st) {
            String str = s.toString();
            if (!str.startsWith("reactor.")
                    && !str.startsWith("java.")
                    && !str.startsWith("com.couchbase.client.core")
                    && sb.length() > 0) {
                sb.append(sb);
                sb.append("; ");
            }
        }
        return sb.toString();
    }

    public static String dbg(@Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
        if (flexibleExtras == null) {
            return "";
        }

        return " using " +
                (flexibleExtras.writeUnits >= 0 ? flexibleExtras.writeUnits : 0)  +
                " WUs " +
                (flexibleExtras.readUnits >= 0 ? flexibleExtras.readUnits : 0)  +
                " RUs";
    }

    public static String dbg(@Nullable MeteringUnits units) {
        if (units == null) {
            return "";
        }

        return " using " +
                (units.writeUnits == null ? 0 : units.writeUnits) +
                " WUs " +
                (units.readUnits == null ? 0 : units.readUnits) +
                " RUs ";

    }
}
