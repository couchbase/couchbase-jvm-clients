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
import com.couchbase.client.core.error.context.KeyValueIoErrorContext;
import com.couchbase.client.core.util.Bytes;

import java.time.Duration;

/**
 * If something goes wrong during the SASL authentication process, this event is raised.
 *
 * <p>Usually when this event is raised, the channel in the context has been closed and
 * the connect process aborted, since without authentication no data can flow. This should
 * be treated as an important problem to be looked at and therefore has an ERROR severity.</p>
 *
 * <p>A little more context might be provided from the description.</p>
 *
 * @since 2.0.0
 */
public class SaslAuthenticationFailedEvent extends AbstractEvent {

  private final String description;

  public SaslAuthenticationFailedEvent(final Duration duration, final KeyValueIoErrorContext context,
                                       final String description) {
    super(Severity.ERROR, Category.IO, duration, context);
    this.description = description;
  }

  /**
   * @deprecated The raw packet is no longer recorded, so {@code lastPacket} is ignored.
   * Please use {@link #SaslAuthenticationFailedEvent(Duration, KeyValueIoErrorContext, String)} instead.
   */
  @Deprecated
  public SaslAuthenticationFailedEvent(final Duration duration, final KeyValueIoErrorContext context,
                                       final String description, final byte[] lastPacket) {
    this(duration, context, description);
  }

  @Override
  public String description() {
    return description;
  }

  /**
   * @deprecated Raw SASL packets are no longer recorded, so that they can never be logged.
   * Any server-supplied error details are available in the {@link KeyValueIoErrorContext}.
   * This always returns an empty array.
   */
  @Deprecated
  public byte[] lastPacket() {
    return Bytes.EMPTY_BYTE_ARRAY;
  }
}
