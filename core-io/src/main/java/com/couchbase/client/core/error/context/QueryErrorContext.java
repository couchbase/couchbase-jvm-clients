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
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.msg.RequestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Stability.Uncommitted
public class QueryErrorContext extends ErrorContext {

  private final RequestContext requestContext;
  private final List<ErrorCodeAndMessage> errors;

  public QueryErrorContext(final RequestContext requestContext, final List<ErrorCodeAndMessage> errors) {
    super(null);
    this.errors = errors;
    this.requestContext = requestContext;
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  public List<ErrorCodeAndMessage> errors() {
    return errors;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (requestContext != null) {
      requestContext.injectExportableParams(input);
    }

    List<Map<String, Object>> errorList = new ArrayList<>(errors.size());
    for (ErrorCodeAndMessage error : errors) {
      Map<String, Object> err = new TreeMap<>();
      err.put("code", error.code());
      err.put("message", error.message());
      if (error.context() != null && !error.context().isEmpty()) {
        err.put("additional", error.context());
      }
      errorList.add(err);
    }
    input.put("errors", errorList);
  }

}
