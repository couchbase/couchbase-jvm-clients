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

import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Map;

/**
 * The ErrorContext is the parent interface for all service-specific error contexts that are thrown as part of
 * the {@link CouchbaseException}.
 */
public abstract class ErrorContext extends AbstractContext {

  private final ResponseStatus responseStatus;

  protected ErrorContext(final ResponseStatus responseStatus) {
    this.responseStatus = responseStatus;
  }

  public ResponseStatus responseStatus() {
    return responseStatus;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (responseStatus != null) {
      input.put("status", responseStatus);
    }
  }

}
