/*
 * Copyright (c) 2023 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.msg.ResponseStatus;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

@Stability.Volatile
public class ProtostellarErrorContext extends ErrorContext {
  private Map<String, Object> fields;
  private @Nullable final AbstractContext ctx;

  public ProtostellarErrorContext(Map<String, Object> input, @Nullable AbstractContext ctx) {
    super(ResponseStatus.UNKNOWN);
    this.fields = input;
    this.ctx = ctx;
  }

  @Stability.Internal
  public void put(String key, Object value) {
    if (fields == null) {
      fields = new HashMap<>();
    }
    fields.put(key, value);
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.putAll(fields);
    if (ctx != null) {
      ctx.injectExportableParams(input);
    }
  }
}
