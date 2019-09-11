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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.endpoint.EndpointContext;

import java.time.Duration;

/**
 * If unordered execution is enabled via a system property, this warning event is raised to make sure the
 * user is aware that this is unsupported for now!
 */
public class UnorderedExecutionEnabledEvent extends AbstractEvent {

  public UnorderedExecutionEnabledEvent(final EndpointContext context) {
    super(Severity.WARN, Category.CONFIG, Duration.ZERO, context);
  }

  @Override
  public String description() {
    return "The unsupported, experimental Unordered Execution has been enabled. There will be dragons!";
  }

}
