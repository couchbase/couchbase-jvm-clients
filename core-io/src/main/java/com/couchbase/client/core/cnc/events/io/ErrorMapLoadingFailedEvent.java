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
 * If loading the KV error map for some reason fails, this event will capture
 * the KV status code.
 */
public class ErrorMapLoadingFailedEvent extends AbstractEvent {

  private final short status;

  public ErrorMapLoadingFailedEvent(final IoContext ctx, Duration duration, short status) {
    super(Severity.WARN, Category.IO, duration, ctx);
    this.status = status;
  }

  @Override
  public String description() {
    return "KV Error Map Negotiation failed (Status 0x" +  Integer.toHexString(status) + ")";
  }

  /**
   * Returns the KV status which was not successful, useful for debugging.
   *
   * @return the unsuccessful status.
   */
  public short status() {
    return status;
  }
}
