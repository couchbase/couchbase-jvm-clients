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

package com.couchbase.client.java;

import com.couchbase.client.core.callbacks.BeforeSendRequestCallback;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import static com.couchbase.client.test.Util.waitUntilCondition;

@IgnoreWhen(isProtostellar = true)
public class BeforeSendCallbackIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Collection collection;
  private static final LongAdder adder = new LongAdder();

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster(env -> env
      .addRequestCallback((BeforeSendRequestCallback) request -> adder.increment())
    );
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  void callsBeforeSendCallback() {
    long before = adder.longValue();

    collection.upsert(UUID.randomUUID().toString(), JsonObject.create());
    collection.upsert(UUID.randomUUID().toString(), JsonObject.create());
    collection.upsert(UUID.randomUUID().toString(), JsonObject.create());

    waitUntilCondition(() -> adder.longValue() >= before + 3);
  }

}
