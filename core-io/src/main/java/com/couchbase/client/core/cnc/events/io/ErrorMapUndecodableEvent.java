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
 * If the KV error map could be loaded, but the decoding of the
 * buffer from JSON failed, this event will be raised.
 */
public class ErrorMapUndecodableEvent extends AbstractEvent {

  private final String message;
  private final String content;

  /**
   * Creates a new {@link ErrorMapUndecodableEvent}.
   *
   * @param ctx the io context for better logging.
   * @param message the error message of the error.
   * @param content the content of the undecodable map.
   */
  public ErrorMapUndecodableEvent(final IoContext ctx, String message, String content) {
    super(Severity.WARN, Category.IO, Duration.ZERO, ctx);
    this.message = message;
    this.content = content;
  }

  @Override
  public String description() {
    return "KV Error Map loaded but undecodable. Message: \"" + message
      + "\", Content: \"" + content + "\"";
  }

  /**
   * Returns the error message in string form.
   *
   * @return error message.
   */
  public String message() {
    return message;
  }

  /**
   * Returns the content in string form.
   *
   * @return undecodable content
   */
  public String content() {
    return content;
  }
}
