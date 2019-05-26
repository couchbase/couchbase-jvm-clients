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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

/**
 * This event is raised if a collection map could be loaded, but afterwards cannot be decoded and used
 * as intended.
 */
public class CollectionMapDecodingFailedEvent extends AbstractEvent {

  private final Throwable reason;

  public CollectionMapDecodingFailedEvent(Context context, Throwable reason) {
    super(Severity.WARN, Category.CONFIG, Duration.ZERO, context);
    this.reason = reason;
  }

  @Override
  public String description() {
    return "Collection Map decoding failed: " + reason;
  }

}
