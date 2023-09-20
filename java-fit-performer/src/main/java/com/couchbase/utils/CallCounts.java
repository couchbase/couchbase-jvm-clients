/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.utils;

import com.couchbase.client.protocol.hooks.transactions.HookPoint;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks what calls to what hooks were made.
 * <p>
 * Thread-safe.
 */
class CallCounts {
    private final Map<HookPoint, LongAdder> countsPerHook = new ConcurrentHashMap<>();
    // So a call to BEFORE_DOC_COMMITTED with "docId1" can be tracked separately to a call to the same hook point with
    // "docId2".
    private final Map<Tuple2<HookPoint, String>, LongAdder> countsPerHookAndParam = new ConcurrentHashMap<>();

    public void add(HookPoint hookPoint) {
        countsPerHook.computeIfAbsent(hookPoint, key -> new LongAdder()).increment();
    }

    public void add(HookPoint hookPoint, String param) {
      countsPerHookAndParam.computeIfAbsent(Tuples.of(hookPoint, param), key -> new LongAdder()).increment();
    }

    public long getCount(HookPoint hookPoint) {
        var adder = countsPerHook.get(hookPoint);
        return adder == null ? 0 : adder.sum();
    }

    public long getCount(HookPoint hookPoint, String param) {
      var adder = countsPerHookAndParam.get(Tuples.of(hookPoint, param));
      return adder == null ? 0 : adder.sum();
    }
}
