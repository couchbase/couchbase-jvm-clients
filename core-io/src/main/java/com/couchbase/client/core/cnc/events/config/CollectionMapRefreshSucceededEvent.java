/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.io.CollectionIdentifier;

import java.time.Duration;

/**
 * This event is raised if an individual collection map refresh attempt succeeded.
 */
public class CollectionMapRefreshSucceededEvent extends AbstractEvent {

  private final CollectionIdentifier collectionIdentifier;
  private final long cid;

  public CollectionMapRefreshSucceededEvent(final Duration duration, final Context context,
                                            final CollectionIdentifier collectionIdentifier,
                                            final long cid) {
    super(Severity.DEBUG, Category.CONFIG, duration, context);
    this.collectionIdentifier = collectionIdentifier;
    this.cid = cid;
  }

  @Override
  public String description() {
    return "CollectionMap refresh succeeded (0x" + Long.toHexString(cid) + ") for identifier: " + collectionIdentifier;
  }
}
