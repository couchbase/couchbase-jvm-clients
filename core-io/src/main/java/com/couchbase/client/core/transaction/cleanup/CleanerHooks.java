/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.cleanup;

import com.couchbase.client.core.annotation.Stability;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.function.Supplier;

@Stability.Internal
public class CleanerHooks {
    public static final CleanerHooks DEFAULT = new CleanerHooks();
    public static final Mono<Integer> standard = Mono.just(1);

    // Just for testing so these are public
    public Function<String, Mono<Integer>> beforeAtrGet = (x) -> standard;
    public Function<String, Mono<Integer>> beforeCommitDoc = (x) -> standard;
    public Function<String, Mono<Integer>> beforeRemoveDocStagedForRemoval = (x) -> standard;
    public Function<String, Mono<Integer>> beforeDocGet = (x) -> standard;
    public Function<String, Mono<Integer>> beforeRemoveDoc = (x) -> standard;
    public Function<String, Mono<Integer>> beforeRemoveLinks = (x) -> standard;
    public Supplier<Mono<Integer>> beforeAtrRemove = () -> standard;
}
