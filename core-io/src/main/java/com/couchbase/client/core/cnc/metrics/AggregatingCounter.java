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

package com.couchbase.client.core.cnc.metrics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Counter;

/**
 * At the moment this is a NOOP implementation because we do not perform any aggregations
 * on the counters.
 */
@Stability.Volatile
public class AggregatingCounter implements Counter {

  static final AggregatingCounter INSTANCE = new AggregatingCounter();

  @Override
  public void incrementBy(long number) { }
}
