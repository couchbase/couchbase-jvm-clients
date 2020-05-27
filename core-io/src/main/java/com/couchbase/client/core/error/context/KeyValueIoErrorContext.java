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
package com.couchbase.client.core.error.context;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Map;

@Stability.Uncommitted
public class KeyValueIoErrorContext extends ErrorContext {

  private final EndpointContext endpointContext;
  private final Map<String, Object> serverContext;

  public KeyValueIoErrorContext(final ResponseStatus responseStatus, final EndpointContext endpointContext,
                                final Map<String, Object> serverContext) {
    super(responseStatus);
    this.endpointContext = endpointContext;
    this.serverContext = serverContext;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);

    if (endpointContext != null) {
      endpointContext.injectExportableParams(input);
    }
    if (serverContext != null && serverContext.get("error") != null) {
      input.put("xerror", serverContext.get("error"));
    }
  }

}
