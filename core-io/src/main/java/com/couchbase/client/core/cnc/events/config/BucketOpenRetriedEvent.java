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

import java.time.Duration;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

/**
 * This event is raised if a bucket could not be opened and is retried, for debugging reasons.
 */
public class BucketOpenRetriedEvent extends AbstractEvent {

  private final String name;
  private final Throwable cause;

  public BucketOpenRetriedEvent(final String name, final Duration duration, final Context context, final Throwable cause) {
    super(Severity.DEBUG, Category.CONFIG, duration, context);
    this.cause = cause;
    this.name = name;
  }

  public String bucketName() {
    return name;
  }

  @Override
  public String description() {
    return "Failed to open bucket [\"" + redactMeta(name) + "\"]";
  }

  @Override
  public Throwable cause() {
    return cause;
  }

}
