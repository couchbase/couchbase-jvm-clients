/*
 * Copyright (c) 2021 Couchbase, Inc.
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
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Map;

@Stability.Uncommitted
public class ManagerErrorContext extends ErrorContext {

  private final RequestContext requestContext;
  private final int httpStatus;
  private final String content;

  public ManagerErrorContext(final ResponseStatus responseStatus, final RequestContext requestContext,
                             final int httpStatus, final String content) {
    super(responseStatus);
    this.requestContext = requestContext;
    this.httpStatus = httpStatus;
    this.content = content;
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  public int httpStatus() {
    return httpStatus;
  }

  public String content() {
    return content;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (requestContext != null) {
      requestContext.injectExportableParams(input);
    }
    input.put("httpStatus", httpStatus);
    if (content != null) {
      input.put("httpBody", content);
    }
  }

}
