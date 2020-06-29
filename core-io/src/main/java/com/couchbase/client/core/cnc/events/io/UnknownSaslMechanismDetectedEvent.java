/*
 * Copyright (c) 2020 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

/**
 * This event is raised if the client detects an unknown SASL mechanism it cannot turn into an enum.
 * <p>
 * Note that this event is at DEBUG because it is perfectly acceptable that in a future server version the server
 * sends a mechanism we don't know yet.
 */
public class UnknownSaslMechanismDetectedEvent extends AbstractEvent {

  private final String mechanism;

  public UnknownSaslMechanismDetectedEvent(final Context context, final String mechanism) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.mechanism = mechanism;
  }

  @Override
  public String description() {
    return "Detected (and ignoring) and unknown SASL mechanism: " + mechanism;
  }

  /**
   * Returns the unknown mechanism.
   */
  public String mechanism() {
    return mechanism;
  }
}
