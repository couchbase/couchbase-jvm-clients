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

/**
 * If the server sends us unsolicited features during the HELLO negotiation,
 * this event will be raised - it is a warning severity since it indicates
 * a server bug.
 */
public class UnsolicitedFeaturesReturnedEvent extends AbstractEvent {

  private final List<ServerFeature> unsolicitedFeatures;

  public UnsolicitedFeaturesReturnedEvent(final IoContext ctx,
                                          final List<ServerFeature> unsolicitedFeatures) {
    super(Severity.WARN, Category.IO, Duration.ZERO, ctx);
    this.unsolicitedFeatures = unsolicitedFeatures;
  }

  @Override
  public String description() {
    return "Received unsolicited features during HELLO " + unsolicitedFeatures.toString();
  }

  /**
   * Returns the unsolicited features that got returned by the server.
   *
   * @return the list of unsolicited features returned by the server.
   */
  public List<ServerFeature> unsolicitedFeatures() {
    return unsolicitedFeatures;
  }

}
