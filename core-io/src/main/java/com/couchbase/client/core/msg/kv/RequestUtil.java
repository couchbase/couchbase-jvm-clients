package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.RequestContext;

import java.time.Duration;

public class RequestUtil {
    private RequestUtil() {}

    // TODO move into env
    // TODO surface persistence_timeout_ceiling
    private static final long PERSISTENCE_TIMEOUT_FLOOR = 1500;

    public static Duration handleDurabilityTimeout(final RequestContext ctx, final Duration timeout) {
        long timeoutMilliseconds = timeout.toMillis();

        if (timeoutMilliseconds < PERSISTENCE_TIMEOUT_FLOOR) {
            // TODO should actually proceed with timeout of PERSISTENCE_TIMEOUT_FLOOR and log an exception
            throw new IllegalArgumentException("Provided timeout " + timeout + " is below minimum of " + PERSISTENCE_TIMEOUT_FLOOR + " milliseconds");
        }

        long adjustedTimeout = (long) (timeoutMilliseconds * 0.9);

        return Duration.ofMillis(adjustedTimeout);
    }
}
