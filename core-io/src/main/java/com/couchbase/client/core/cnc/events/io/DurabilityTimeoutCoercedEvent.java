package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

public class DurabilityTimeoutCoercedEvent extends AbstractEvent {
    final long requestedTimeout;
    final long actualTimeout;


    public DurabilityTimeoutCoercedEvent(Context context, long requestedTimeout, long actualTimeout ) {
        super(Severity.WARN, Category.IO,  Duration.ZERO, context);
        this.requestedTimeout = requestedTimeout;
        this.actualTimeout = actualTimeout;
    }

    @Override
    public String description() {
        return("Requested DurabilityTimeout outside range (1700-65535ms), using client side timeout of " + requestedTimeout + "ms and server-side timeout of " + actualTimeout + "ms.");
    }

}
