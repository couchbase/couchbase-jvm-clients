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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

/**
 * Reports a bucket open failure which has not been retried.
 */
public class BucketOpenFailedEvent extends AbstractEvent {

  private final String name;
  private final Throwable cause;

  public BucketOpenFailedEvent(String name, Severity severity, Duration duration, Context context, Throwable cause) {
    super(severity, Category.CORE, duration, context);
    this.cause = cause;
    this.name = name;
  }

  @Override
  public String description() {
    return "Opening bucket [\""+name+"\"] failed";
  }

  @Override
  public Throwable cause() {
    return cause;
  }

}
