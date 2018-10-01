package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;

public class FeaturesNegotiatedEvent extends AbstractEvent {

    public FeaturesNegotiatedEvent(final IoContext ctx, final Duration duration) {
        super(Severity.DEBUG, Category.IO, duration, ctx);
    }
}
