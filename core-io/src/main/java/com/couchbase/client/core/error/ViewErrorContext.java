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

package com.couchbase.client.core.error;

import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.view.ViewError;

import java.util.Map;

public class ViewErrorContext extends ErrorContext {

  private final RequestContext requestContext;
  private final ViewError viewError;

  public ViewErrorContext(RequestContext requestContext, ViewError viewError) {
    super(null);
    this.requestContext = requestContext;
    this.viewError = viewError;
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  public ViewError viewError() {
    return viewError;
  }

  @Override
  public void injectExportableParams(Map<String, Object> input) {
    super.injectExportableParams(input);
    if (requestContext != null) {
      requestContext.injectExportableParams(input);
    }
    if (viewError != null) {
      input.put("viewError", viewError.error());
      input.put("viewErrorReason", viewError.reason());
    }
  }
}
