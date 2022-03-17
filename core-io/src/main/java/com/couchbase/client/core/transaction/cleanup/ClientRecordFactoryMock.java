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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * Used for testing/mocking.  Creates a mocked ClientRecord.
 *
 * @author Graham Pople
 */
@Stability.Internal
public class ClientRecordFactoryMock extends ClientRecordFactory {
    private Mono<Integer> standard = Mono.just(1);
    public Supplier<Mono<Integer>> beforeCreateRecord = () -> standard;
    public Supplier<Mono<Integer>> beforeRemoveClient = () -> standard;
    public Supplier<Mono<Integer>> beforeGetRecord = () -> standard;
    public Supplier<Mono<Integer>> beforeUpdateRecord = () -> standard;

    public ClientRecord create(Core core) {
        return new ClientRecord(core) {
            @Override
            protected Mono<Integer> beforeCreateRecord(ClientRecord self) {
                return Mono.defer(() -> beforeCreateRecord.get());
            }

            @Override
            protected Mono<Integer> beforeRemoveClient(ClientRecord self) {
                return Mono.defer(() -> beforeRemoveClient.get());
            }

            @Override
            protected Mono<Integer> beforeGetRecord(ClientRecord self) {
                return Mono.defer(() -> beforeGetRecord.get());
            }

            @Override
            protected Mono<Integer> beforeUpdateRecord(ClientRecord self) {
                return Mono.defer(() -> beforeUpdateRecord.get());
            }
        };
    }
}
