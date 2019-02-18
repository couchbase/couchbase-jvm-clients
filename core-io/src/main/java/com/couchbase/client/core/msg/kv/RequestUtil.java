package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.RequestContext;

import java.time.Duration;

public class RequestUtil {
    private RequestUtil() {}

    // TODO move into env
    private static final long PERSISTENCE_TIMEOUT_FLOOR = 1500;

    public static Duration handleDurabilityTimeout(final RequestContext ctx, final Duration timeout) {
        long timeoutMilliseconds = timeout.toMillis();
        long adjustedTimeout = (long) (timeoutMilliseconds * 0.9);

        if (adjustedTimeout < PERSISTENCE_TIMEOUT_FLOOR) {
            throw new IllegalArgumentException("Provided timeout, after adjustment, is below minimum of " + PERSISTENCE_TIMEOUT_FLOOR + " milliseconds");
        }

        return Duration.ofMillis(adjustedTimeout);
    }
}
