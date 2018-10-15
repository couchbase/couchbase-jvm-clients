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
import com.couchbase.client.core.io.netty.kv.ErrorMap;

import java.time.Duration;
import java.util.Optional;

/**
 * The KV error map got negotiated loaded from the server.
 *
 * <p>Note that it still can be empty (so we got a successful response from the server
 * but no map was provided).</p>
 */
public class ErrorMapLoadedEvent extends AbstractEvent {

  private final Optional<ErrorMap> errorMap;

  public ErrorMapLoadedEvent(final IoContext ctx, final Duration duration,
                             final Optional<ErrorMap> errorMap) {
    super(Severity.DEBUG, Category.IO, duration, ctx);
    this.errorMap = errorMap;
  }

  /**
   * Returns the error map if present.
   *
   * @return the error map if present.
   */
  public Optional<ErrorMap> errorMap() {
    return errorMap;
  }

  @Override
  public String description() {
    return errorMap
      .map(m -> "KV Error Map successfully loaded. Map Version: "
        + m.version() + ", Revision: " + m.revision())
      .orElse("KV Error Map successfully negotiated, but no map found in body");
  }
}
