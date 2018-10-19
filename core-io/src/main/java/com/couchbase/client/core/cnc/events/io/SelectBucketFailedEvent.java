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

package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;

/**
 * If selecting a bucket during KV bootstrap fails, this is a really problematic
 * issue and needs to be looked at.
 *
 * @since 2.0.0
 */
public class SelectBucketFailedEvent extends AbstractEvent {

  /**
   * Holds the the description for this select bucket failed event.
   */
  private final short status;

  public SelectBucketFailedEvent(final IoContext context, final short status) {
    super(Severity.ERROR, Category.IO, Duration.ZERO, context);
    this.status = status;
  }

  public short status() {
    return status;
  }

  @Override
  public String description() {
    return "Select bucket failed with status code 0x" + Integer.toHexString(status);
  }
}
