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
package com.couchbase.client.core.cnc.events.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Event;

import java.time.Duration;

/**
 * All transaction events derive from this.
 * <p>
 * TransactionEvents at WARN or ERROR level are intended for serious issues that should be investigated, e.g. they
 * should not just be logged.  All applications should be listening for and handling events at this level.
 * <p>
 * At INFO or below they can convey various events that the application can ignore, but may be useful to be
 * programmatically handled.
 */
@Stability.Uncommitted
public abstract class TransactionEvent extends AbstractEvent {
    public static String DEFAULT_CATEGORY = Event.CATEGORY_PREFIX + "transactions";

    protected TransactionEvent(Severity severity, String category) {
        super(severity, category, Duration.ZERO, null);
    }
}
