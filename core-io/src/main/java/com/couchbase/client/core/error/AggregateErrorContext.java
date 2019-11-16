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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An error context which combines more than one context to dump.
 */
public class AggregateErrorContext extends ErrorContext {

  private final List<ErrorContext> innerContexts;

  public AggregateErrorContext(final List<ErrorContext> innerContexts) {
    super(null);
    this.innerContexts = innerContexts;
  }

  public List<ErrorContext> contexts() {
    return innerContexts;
  }

  @Override
  public void injectExportableParams(Map<String, Object> input) {
    super.injectExportableParams(input);
    if (innerContexts != null && !innerContexts.isEmpty()) {
      List<Map<String, Object>> inner = new ArrayList<>();
      for (ErrorContext ctx : innerContexts) {
        Map<String, Object> ctxInner = new HashMap<>();
        ctx.injectExportableParams(ctxInner);
        inner.add(ctxInner);
      }
      input.put("aggregateErrors", inner);
    }
  }
}
