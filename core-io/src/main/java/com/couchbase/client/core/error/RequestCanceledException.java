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

package com.couchbase.client.core.error;

import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.msg.RequestContext;

import java.util.concurrent.CancellationException;

public class RequestCanceledException extends CancellationException implements RetryableOperationException {

  private final String name;
  private final RequestContext requestContext;

  public RequestCanceledException(String name, RequestContext context) {
    this.requestContext = context;
    this.name = name;
  }

  public String name() {
    return name;
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  @Override
  public String getMessage() {
    return name + " " + requestContext.exportAsString(Context.ExportFormat.JSON);
  }
}
