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
package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.retry.RetryReason;

import java.time.Duration;

public class CollectionOutdatedHandledEvent extends AbstractEvent {

  private final CollectionIdentifier collectionIdentifier;
  private final RetryReason retryReason;

  public CollectionOutdatedHandledEvent(CollectionIdentifier collectionIdentifier, RetryReason retryReason,
                                        IoContext context) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.collectionIdentifier = collectionIdentifier;
    this.retryReason = retryReason;
  }

  @Override
  public String description() {
    return "Handled collection outdated (or not found) notification for " + collectionIdentifier + " (" + retryReason + ").";
  }



}
