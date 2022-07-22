/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.cnc.AbstractContext;

import java.util.Map;

public class RangeScanContext extends AbstractContext  {

  private final long itemsStreamed;

  public RangeScanContext(long itemsStreamed) {
    this.itemsStreamed = itemsStreamed;
  }

  public long itemsStreamed() {
    return itemsStreamed;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);

    input.put("itemsStreamed", itemsStreamed);
  }
}
