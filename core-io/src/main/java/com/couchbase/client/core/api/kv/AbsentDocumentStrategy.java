/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.TracingIdentifiers;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public enum AbsentDocumentStrategy {
  RETURN_NULL(TracingIdentifiers.SPAN_REQUEST_KV_GET_OR_NULL),
  THROW_EXCEPTION(TracingIdentifiers.SPAN_REQUEST_KV_GET),
  ;

  public final String spanName;

  AbsentDocumentStrategy(String tracingIdentifier) {
    this.spanName = requireNonNull(tracingIdentifier);
  }
}
