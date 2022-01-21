/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.endpoint;

import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.endpoint.EndpointContext;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This event is usually raised if the server closes the socket and the client didn't expect it.
 */
public class UnexpectedEndpointDisconnectedEvent extends AbstractEvent {

  /**
   * How many requests were outstanding when the socket is closed.
   */
  private final int outstandingBeforeInactive;

  /**
   * For how long the socket was connected in nanoseconds.
   */
  private final long connectedForNs;

  private final EndpointContext endpointContext;

  public UnexpectedEndpointDisconnectedEvent(final EndpointContext context, final int outstandingBeforeInactive,
                                             final long connectedForNs) {
    super(Severity.WARN, Category.ENDPOINT, Duration.ZERO, context);
    this.outstandingBeforeInactive = outstandingBeforeInactive;
    this.connectedForNs = connectedForNs;
    this.endpointContext = context;
  }

  @Override
  public String description() {
    return "The remote side disconnected the endpoint unexpectedly";
  }

  @Override
  public Context context() {
    return new AbstractContext() {
      @Override
      public void injectExportableParams(final Map<String, Object> input) {
        endpointContext.injectExportableParams(input);

        input.put("connectedForMs", TimeUnit.NANOSECONDS.toMillis(connectedForNs));
        input.put("numOutstandingRequests", outstandingBeforeInactive);
      }
    };
  }

}
