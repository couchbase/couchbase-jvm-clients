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
 * This warning indicates that the client closed an active IO channel/socket proactively because
 * it detected an invalid state.
 *
 * <p>This situation may or may not clear itself up depending on the cause, but these kinds
 * of errors need to be investigated because normally they should not occur.</p>
 *
 * @since 2.0.0
 */
public class ChannelClosedProactivelyEvent extends AbstractEvent {

  private final Reason reason;

  public ChannelClosedProactivelyEvent(final IoContext context, final Reason reason) {
    super(Severity.WARN, Category.IO, Duration.ZERO, context);
    this.reason = reason;
  }

  public Reason reason() {
    return reason;
  }

  @Override
  public String description() {
    return "Proactively closed the channel because of: " + reason;
  }

  /**
   * Specifies the reasons why a channel has been proactively closed by the SDK.
   */
  public enum Reason {
    /**
     * We detected an invalid request coming in from the upper layers.
     */
    INVALID_REQUEST_DETECTED,
    /**
     * We got a KV response which contained an opaque value that the client has no idea about.
     */
    KV_RESPONSE_CONTAINED_UNKNOWN_OPAQUE,
    /**
     * We got a KV response which was not successful and indicated that the socket/channel needs to be reset.
     */
    KV_RESPONSE_CONTAINED_CLOSE_INDICATION,
    /**
     * Got a response format the decoder didn't expect.
     */
    INVALID_RESPONSE_FORMAT_DETECTED
  }
}
