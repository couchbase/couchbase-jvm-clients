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
import com.couchbase.client.core.io.netty.kv.ServerFeature;

import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Captures the end of the KV feature negotiation.
 */
public class FeaturesNegotiatedEvent extends AbstractEvent {

  private final List<ServerFeature> negotiated;

  public FeaturesNegotiatedEvent(final IoContext ctx, final Duration duration,
                                 final List<ServerFeature> negotiated) {
    super(Severity.DEBUG, Category.IO, duration, ctx);
    this.negotiated = requireNonNull(negotiated);
  }

  @Override
  public String description() {
    return "Negotiated " + negotiated;
  }

  /**
   * Returns the negotiated server features for this connection.
   *
   * @return the negotiated list of server features.
   */
  public List<ServerFeature> negotiated() {
    return negotiated;
  }

}
