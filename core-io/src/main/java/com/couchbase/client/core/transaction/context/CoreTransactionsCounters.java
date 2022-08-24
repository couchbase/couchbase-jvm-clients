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
package com.couchbase.client.core.transaction.context;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.util.CbCollections;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.cnc.TracingIdentifiers.METER_TRANSACTION_ATTEMPTS;
import static com.couchbase.client.core.cnc.TracingIdentifiers.METER_TRANSACTION_TOTAL;
import static com.couchbase.client.core.cnc.TracingIdentifiers.SERVICE_TRANSACTIONS;

@Stability.Internal
public class CoreTransactionsCounters {
  private final Counter transactions;
  private final Counter attempts;

  public CoreTransactionsCounters(Meter meter) {
    Map<String, String> tags = CbCollections.mapOf(TracingIdentifiers.ATTR_SERVICE, SERVICE_TRANSACTIONS);
    transactions = meter.counter(METER_TRANSACTION_TOTAL, tags);
    attempts = meter.counter(METER_TRANSACTION_ATTEMPTS, tags);
  }

  public Counter attempts() {
    return attempts;
  }

  public Counter transactions() {
    return transactions;
  }
}
