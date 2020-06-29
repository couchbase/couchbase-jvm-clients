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
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;
import java.util.Set;

/**
 * This debug event signals that SASL auth mechanisms have been listed from the server.
 */
public class SaslMechanismsListedEvent extends AbstractEvent {

  private final Set<SaslMechanism> serverMechanisms;

  public SaslMechanismsListedEvent(final IoContext context, final Set<SaslMechanism> serverMechanisms,
                                   final Duration duration) {
    super(Severity.DEBUG, Category.IO, duration, context);
    this.serverMechanisms = serverMechanisms;
  }

  @Override
  public String description() {
    return "Listed SASL mechanisms: " + serverMechanisms;
  }

  public Set<SaslMechanism> serverMechanisms() {
    return serverMechanisms;
  }

}
