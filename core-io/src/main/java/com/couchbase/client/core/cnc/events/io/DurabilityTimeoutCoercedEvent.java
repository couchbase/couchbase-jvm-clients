/*
 * Copyright (c) 2019 Couchbase, Inc.
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
