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
import com.couchbase.client.protocol.shared.DocLocation;
import com.couchbase.client.protocol.shared.RandomDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.couchbase.client.performer.core.util.ErrorUtil.convertException;

public abstract class SdkCommandExecutor {
    protected final Logger logger = LoggerFactory.getLogger(SdkCommandExecutor.class);
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final Counters counters;

    public SdkCommandExecutor(Counters counters) {
        this.counters = counters;
    }

    abstract protected com.couchbase.client.protocol.run.Result performOperation(com.couchbase.client.protocol.sdk.Command op);

    abstract protected com.couchbase.client.protocol.shared.Exception convertException(Throwable raw);

    // Returns a com.couchbase.client.protocol.run.Result rather than a com.couchbase.client.protocol.run.Result directly, so it
    // can also return the timing info.
    public com.couchbase.client.protocol.run.Result run(com.couchbase.client.protocol.sdk.Command command) {
        try {
            return performOperation(command);
        }
        catch (RuntimeException err) {
            return com.couchbase.client.protocol.run.Result.newBuilder()
                    .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                            .setException(convertException(err)))
                    .build();
        }
    }

    protected String getDocId(DocLocation location) {
        if (location.hasSpecific()) {
            return location.getSpecific().getId();
        }
        else if (location.hasUuid()) {
            return UUID.randomUUID().toString();
        }
        else if (location.hasPool()) {
            var pool = location.getPool();

            int next;
            if (pool.hasRandom()) {
                if (pool.getRandom().getDistribution() == RandomDistribution.RANDOM_DISTRIBUTION_UNIFORM) {
                    next = random.nextInt((int) pool.getPoolSize());
                }
                else {
                    throw new UnsupportedOperationException("Unrecognised random distribution");
                }
            }
            else if (pool.hasCounter()) {
                var counter = counters.getCounter(pool.getCounter().getCounter());
                next = counter.getAndIncrement() % (int) pool.getPoolSize();
            }
            else {
                throw new UnsupportedOperationException("Unrecognised pool selection strategy");
            }

            return pool.getIdPreface() + next;
        }
        else {
            throw new UnsupportedOperationException("Unknown doc location type");
        }
    }
}
