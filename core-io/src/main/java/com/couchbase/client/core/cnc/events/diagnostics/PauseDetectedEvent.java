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

package com.couchbase.client.core.cnc.events.diagnostics;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

/**
 * When the analyzer detects a JVM pause, an event will be raised.
 */
public class PauseDetectedEvent extends AbstractEvent {

  public PauseDetectedEvent(Severity severity, Duration duration, Context context) {
    super(severity, Category.SYSTEM, duration, context);
  }

  @Override
  public String description() {
    return "Detected a JVM pause.";
  }
}
