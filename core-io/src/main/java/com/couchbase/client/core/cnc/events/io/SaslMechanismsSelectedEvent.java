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
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;
import java.util.Set;

/**
 * This debug event signals that a SASL auth mechanism has been negotiated
 * between the client and the server.
 *
 * <p>This also contains more information around which mechansims are supported
 * or advertised by the server and what the client was configured to accept.</p>
 *
 * @since 2.0.0
 */
public class SaslMechanismsSelectedEvent extends AbstractEvent {

  private final Set<SaslMechanism> allowedMechanisms;
  private final SaslMechanism selectedMechanism;

  public SaslMechanismsSelectedEvent(final IoContext context,
                                     final Set<SaslMechanism> allowedMechanisms,
                                     final SaslMechanism selectedMechanism) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.allowedMechanisms = allowedMechanisms;
    this.selectedMechanism = selectedMechanism;
  }

  @Override
  public String description() {
    return "SASL Mechanism " + selectedMechanism + " selected. Allowed: " + allowedMechanisms;
  }

  public Set<SaslMechanism> allowedMechanisms() {
    return allowedMechanisms;
  }

  public SaslMechanism selectedMechanism() {
    return selectedMechanism;
  }
}
