/*
 * Copyright (c) 2023 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.endpoint.EndpointContext;

import java.time.Duration;

/**
 * This event is generated when an endpoint disconnect was delayed, but is now about to be disconnected
 *
 */
@Stability.Internal
public class EndpointDisconnectResumedEvent extends AbstractEvent {

  public EndpointDisconnectResumedEvent(final EndpointContext context) {
    super(Severity.DEBUG, Category.ENDPOINT, Duration.ZERO, context);
  }

  @Override
  public String description() {
    return "Endpoint disconnect resumed after being delayed";
  }
}
