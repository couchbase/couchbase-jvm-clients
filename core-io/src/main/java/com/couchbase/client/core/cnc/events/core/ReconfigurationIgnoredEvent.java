/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

/**
 * Client got a new config, but for some valid reason the event is ignored.
 *
 * @since 2.0.0
 * @deprecated This event is never emitted by the SDK.
 */
@Deprecated
public class ReconfigurationIgnoredEvent extends AbstractEvent {

  public ReconfigurationIgnoredEvent(Context context) {
    super(Severity.VERBOSE, Category.CORE, Duration.ZERO, context);
  }

  @Override
  public String description() {
    return "Reconfiguration ignored, since one is in progress or already shut down.";
  }

}
