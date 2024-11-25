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
package com.couchbase.client.core.transaction.context;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Meter;

/**
 * Holds transactions state that has the same lifetime as a Core.
 *
 * Other transactions components (such as cleanup) will be moved here in future.
 */
@Stability.Internal
public class CoreTransactionsContext {
  private final CoreTransactionsCounters counters;

  public CoreTransactionsContext(Core core, Meter meter) {
    this.counters = new CoreTransactionsCounters(core, meter);
  }

  public CoreTransactionsCounters counters() {
    return counters;
  }
}
