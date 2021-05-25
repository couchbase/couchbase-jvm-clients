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

package com.couchbase.client.java.batch;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.msg.kv.MultiObserveViaCasRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides additional insight into why a {@link BatchHelper} operation fails.
 */
@Stability.Volatile
public class BatchErrorContext extends ErrorContext {

  private final List<MultiObserveViaCasRequest> requests;

  BatchErrorContext(final List<MultiObserveViaCasRequest> requests) {
    super(null);
    this.requests = requests;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);

    List<Object> requestContexts = new ArrayList<>();
    for (MultiObserveViaCasRequest request : requests) {
      Map<String, Object> rcx = new HashMap<>();
      request.context().injectExportableParams(rcx);
      requestContexts.add(rcx);
    }

    input.put("requests", requestContexts);
  }

}
