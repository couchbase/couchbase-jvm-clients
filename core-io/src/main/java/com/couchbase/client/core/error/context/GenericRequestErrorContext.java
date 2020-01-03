/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.msg.Request;

import java.util.Map;

/**
 * This generic error context can be constructed from any request and will pull out as much information
 * as possible.
 */
public class GenericRequestErrorContext extends ErrorContext {

  private final Request<?> request;

  public GenericRequestErrorContext(final Request<?> request) {
    super(null);
    this.request = request;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (request != null) {
      request.context().injectExportableParams(input);
    }
  }

}
