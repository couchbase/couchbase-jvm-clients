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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Executor {
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final Counters counters;

    public Executor(Counters counters) {
        this.counters = counters;
    }

    public String getDocId(DocLocation location) {
        if (location.hasSpecific()) {
            return location.getSpecific().getId();
        } else if (location.hasUuid()) {
            return UUID.randomUUID().toString();
        } else if (location.hasPool()) {
            var pool = location.getPool();

            int next;
            if (pool.hasRandom()) {
                if (pool.getRandom().getDistribution() == RandomDistribution.RANDOM_DISTRIBUTION_UNIFORM) {
                    next = random.nextInt((int) pool.getPoolSize());
                } else {
                    throw new UnsupportedOperationException("Unrecognised random distribution");
                }
            } else if (pool.hasCounter()) {
                var counter = counters.getCounter(pool.getCounter().getCounter());
                next = counter.getAndIncrement() % (int) pool.getPoolSize();
            } else {
                throw new UnsupportedOperationException("Unrecognised pool selection strategy");
            }

            return pool.getIdPreface() + next;
        } else {
            throw new UnsupportedOperationException("Unknown doc location type");
        }
    }
}
