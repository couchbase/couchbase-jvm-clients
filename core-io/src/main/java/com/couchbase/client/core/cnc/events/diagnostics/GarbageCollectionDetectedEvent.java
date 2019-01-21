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
import com.couchbase.client.core.cnc.diagnostics.GcAnalyzer;

import java.time.Duration;

public class GarbageCollectionDetectedEvent extends AbstractEvent  {

  private final String action;
  private final String cause;
  private final GcAnalyzer.GcType type;
  private final long memoryBefore;
  private final long memoryAfter;

  public GarbageCollectionDetectedEvent(Severity severity, Duration duration,
                                        String action, String cause, GcAnalyzer.GcType type,
                                        long memoryBefore, long memoryAfter) {
    super(severity, Category.SYSTEM, duration, null);
    this.action = action;
    this.cause = cause;
    this.type = type;
    this.memoryBefore = memoryBefore;
    this.memoryAfter = memoryAfter;
  }

  public String action() {
    return action;
  }

  public String cause() {
    return cause;
  }

  public GcAnalyzer.GcType type() {
    return type;
  }

  public long memoryBefore() {
    return memoryBefore;
  }

  public long memoryAfter() {
    return memoryAfter;
  }

  @Override
  public String description() {
    return "GC event detected " + "{" +
      "action='" + action + '\'' +
      ", cause='" + cause + '\'' +
      ", type='" + type + '\'' +
      ", memoryBefore(mb)=" + memoryBefore / 1000000 +
      ", memoryAfter(mb)=" + memoryAfter / 1000000 +
      '}';
  }

}
