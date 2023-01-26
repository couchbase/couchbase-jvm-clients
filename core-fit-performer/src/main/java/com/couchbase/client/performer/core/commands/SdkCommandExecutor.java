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
package com.couchbase.client.performer.core.commands;

import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

// A useful abstraction layer between the core performer and the per-SDK performers.  Allows refactoring most of the logic
// into the core performer.
// Note it's tempting to put shared things from core-io here, such as RequestSpans, but for technical reasons
// we cannot make the core FIT performer depend on core-io, without making it impossible to test different versions of it.
public abstract class SdkCommandExecutor extends Executor {
    protected final Logger logger = LoggerFactory.getLogger(SdkCommandExecutor.class);
    private final Set<String> errorsSeen = new HashSet<>();

    public SdkCommandExecutor(Counters counters) {
        super(counters);
    }

    abstract protected com.couchbase.client.protocol.run.Result performOperation(com.couchbase.client.protocol.sdk.Command op, PerRun perRun);

    abstract protected com.couchbase.client.protocol.shared.Exception convertException(Throwable raw);

    // Returns a com.couchbase.client.protocol.run.Result so it can also return the timing info.
    public com.couchbase.client.protocol.run.Result run(com.couchbase.client.protocol.sdk.Command command, PerRun perRun) {
        try {
            return performOperation(command, perRun);
        } catch (RuntimeException err) {
            if (err instanceof UnsupportedOperationException) {
                // The perf test can easily create hundreds of thousands of these errors per second, creating gigabytes of logging very quickly.
                // So only log the first example of each error.
                // This isn't thread-safe for performance so the first error could log a few times.
                if (!errorsSeen.contains(err.getMessage())) {
                    logger.warn("Failed to perform unsupported operation: {}", command, err);
                    errorsSeen.add(err.getMessage());
                }
            }

            return com.couchbase.client.protocol.run.Result.newBuilder()
                    .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                            .setException(convertException(err)))
                    .build();
        }
    }
}
