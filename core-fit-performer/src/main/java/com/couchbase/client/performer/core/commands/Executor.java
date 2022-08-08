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
