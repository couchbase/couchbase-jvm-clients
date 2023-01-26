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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.protostellar.util.ProtostellarTestEnvironment;
import com.couchbase.client.protostellar.internal.hooks.v1.AddHooksRequest;
import com.couchbase.client.protostellar.internal.hooks.v1.CreateHooksContextRequest;
import com.couchbase.client.protostellar.internal.hooks.v1.DestroyHooksContextRequest;
import com.couchbase.client.protostellar.internal.hooks.v1.Hook;
import com.couchbase.client.protostellar.internal.hooks.v1.HookAction;
import com.couchbase.client.protostellar.internal.hooks.v1.SignalBarrierRequest;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Verifies that the orphan reporter works with Protostellar.
 */
@Disabled // Currently not working
public class ProtostellarOrphanReporterTest extends ClusterAwareIntegrationTest {

  @Test
  void test() {
    try (ProtostellarTestEnvironment env = ProtostellarTestEnvironment.create(config())) {

      String hooksContextId = UUID.randomUUID().toString();
      String barrierId = UUID.randomUUID().toString();

      env.core().protostellar().endpoint().hooksBlockingStub().createHooksContext(CreateHooksContextRequest.newBuilder().setId(hooksContextId).build());

      try {
        env.core().protostellar().endpoint().hooksBlockingStub().addHooks(AddHooksRequest.newBuilder()
          .setHooksContextId(hooksContextId)
          .addHooks(Hook.newBuilder()
            .setTargetMethod("") // todo what here?
            .addActions(HookAction.newBuilder()
              .setWaitOnBarrier(HookAction.WaitOnBarrier.newBuilder()
                .setBarrierId(barrierId))
              .build())
          )
          .build());

        Mono<CoreGetResult> result = env.ops().getReactive(CoreCommonOptions.DEFAULT, UUID.randomUUID().toString(), new ArrayList<>(), false);

        try {
          result.block();
        } catch (TimeoutException err) {
          env.core().protostellar().endpoint().hooksBlockingStub().signalBarrier(SignalBarrierRequest.newBuilder()
            .setBarrierId(barrierId)
            .build());

          // todo wait for and validate orphan response
        }
      } finally {
        env.core().protostellar().endpoint().hooksBlockingStub().destroyHooksContext(DestroyHooksContextRequest.newBuilder().setId(hooksContextId).build());
      }
    }
  }
}
