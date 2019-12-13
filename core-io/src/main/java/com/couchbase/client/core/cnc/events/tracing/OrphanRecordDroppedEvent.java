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

package com.couchbase.client.core.cnc.events.tracing;

import com.couchbase.client.core.cnc.AbstractEvent;

import java.time.Duration;

/**
 * The client had to drop the orphan record because there queue is already full with processing other orphaned
 * events.
 * <p>
 * Pro tip: instead of making the queue larger (1024 by default), the source of the orphans (likely timeouts) should
 * be investigated instead.
 */
public class OrphanRecordDroppedEvent extends AbstractEvent {

  private final Class<?> requestClass;

  public OrphanRecordDroppedEvent(final Class<?> requestClass) {
    super(Severity.DEBUG, Category.TRACING, Duration.ZERO, null);
    this.requestClass = requestClass;
  }

  @Override
  public String description() {
    return "Orphan record (for \"" + requestClass + "\") had to be dropped because orphan " +
      "queue is already over capacity.";
  }

}
