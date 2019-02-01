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

package com.couchbase.client.core.cnc.events.node;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

public class NodeDisconnectIgnoredEvent extends AbstractEvent {

  private final Reason reason;

  public NodeDisconnectIgnoredEvent(final Severity severity, final Reason reason,
                                    final Context context) {
    super(severity, Category.NODE, Duration.ZERO, context);
    this.reason = reason;
  }

  public Reason reason() {
    return reason;
  }

  @Override
  public String description() {
    return "Node disconnected ignored. Reason: " + reason;
  }

  /**
   * Enumerates the reasons why a disconnect nod event could be ignored by the system.
   */
  public enum Reason {
    /**
     * Disconnect node ignored because the node is disconnected already.
     */
    DISCONNECTED
  }
}
