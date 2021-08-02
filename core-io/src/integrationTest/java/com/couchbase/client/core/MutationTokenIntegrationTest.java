/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.AppendRequest;
import com.couchbase.client.core.msg.kv.AppendResponse;
import com.couchbase.client.core.msg.kv.DecrementRequest;
import com.couchbase.client.core.msg.kv.DecrementResponse;
import com.couchbase.client.core.msg.kv.IncrementRequest;
import com.couchbase.client.core.msg.kv.IncrementResponse;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.PrependRequest;
import com.couchbase.client.core.msg.kv.PrependResponse;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.RemoveResponse;
import com.couchbase.client.core.msg.kv.ReplaceRequest;
import com.couchbase.client.core.msg.kv.ReplaceResponse;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateResponse;
import com.couchbase.client.core.msg.kv.UpsertRequest;
import com.couchbase.client.core.msg.kv.UpsertResponse;
import com.couchbase.client.core.util.CoreIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MutationTokenIntegrationTest extends CoreIntegrationTest {

  private static Core core;
  private static CoreEnvironment env;
  private static CollectionIdentifier collectionIdentifier;

  @BeforeAll
  static void beforeAll() {
    env = environment().ioConfig(IoConfig.enableMutationTokens(true)).build();
    core = Core.create(env, authenticator(), seedNodes());
    core.openBucket(config().bucketname());
    collectionIdentifier = CollectionIdentifier.fromDefault(config().bucketname());
    waitUntilReady(core);
  }

  @AfterAll
  static void afterAll() {
    core.shutdown().block();
    env.shutdown();
  }

  @Test
  void tokenOnAppend() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello".getBytes(UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, content,
      0, false, 0, kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), Optional.empty(), null);
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    AppendRequest appendRequest = new AppendRequest(kvTimeout, core.context(),
      collectionIdentifier, env.retryStrategy(), id,
      ", world".getBytes(UTF_8), upsertResponse.cas(), Optional.empty(), null);
    core.send(appendRequest);

    AppendResponse appendResponse = appendRequest.response().get();
    assertTrue(appendResponse.status().success());
    assertMutationToken(appendResponse.mutationToken());
  }

  @Test
  void tokenOnPrepend() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello".getBytes(UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, content,
      0, false, 0, kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), Optional.empty(), null);
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    PrependRequest prependRequest = new PrependRequest(kvTimeout, core.context(),
      collectionIdentifier, env.retryStrategy(), id,
      ", world".getBytes(UTF_8), upsertResponse.cas(), Optional.empty(), null);
    core.send(prependRequest);

    PrependResponse prependResponse = prependRequest.response().get();
    assertTrue(prependResponse.status().success());
    assertMutationToken(prependResponse.mutationToken());
  }

  @Test
  void tokenOnSubdocMutate() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "{}".getBytes(UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, content,
      0, false, 0, kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), Optional.empty(), null);
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    List<SubdocMutateRequest.Command> commands = new ArrayList<>();
    commands.add(new SubdocMutateRequest.Command(
      SubdocCommandType.DICT_ADD,
      "foo",
      "\"bar\"".getBytes(UTF_8),
      true,
      false,
            false,
            0)
    );
    SubdocMutateRequest subdocMutateRequest = new SubdocMutateRequest(kvTimeout,
      core.context(), collectionIdentifier, null, env.retryStrategy(), id,
      false, false, false, false, commands, 0, false, 0, Optional.empty(), null);
    core.send(subdocMutateRequest);

    SubdocMutateResponse subdocMutateResponse = subdocMutateRequest.response().get();
    assertTrue(subdocMutateResponse.status().success());
    assertMutationToken(subdocMutateResponse.mutationToken());
  }

  @Test
  void tokenOnUpsert() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, content,
      0, false, 0, kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), Optional.empty(), null);
    core.send(upsertRequest);

    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());
  }

  @Test
  void tokenOnReplace() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, content,
      0, false, 0, kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), Optional.empty(), null);
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    ReplaceRequest replaceRequest = new ReplaceRequest(id, content,
      0, false, 0, kvTimeout, upsertResponse.cas(), core.context(),
      collectionIdentifier, env.retryStrategy(), Optional.empty(), null);
    core.send(replaceRequest);

    ReplaceResponse replaceResponse = replaceRequest.response().get();
    assertTrue(replaceResponse.status().success());
    assertMutationToken(replaceResponse.mutationToken());
  }

  @Test
  void tokenOnRemove() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, content,
      0, false, 0, kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), Optional.empty(), null);
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    RemoveRequest removeRequest = new RemoveRequest(id, upsertResponse.cas(),
      kvTimeout, core.context(), collectionIdentifier, env.retryStrategy(), Optional.empty(), null);
    core.send(removeRequest);

    RemoveResponse removeResponse = removeRequest.response().get();
    assertTrue(removeResponse.status().success());
    assertMutationToken(removeResponse.mutationToken());
  }

  @Test
  void tokenOnInsert() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      kvTimeout, core.context(), collectionIdentifier, env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());
    assertMutationToken(insertResponse.mutationToken());
  }

  @Test
  void tokenOnIncrement() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "1".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      kvTimeout, core.context(), collectionIdentifier, env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());
    assertMutationToken(insertResponse.mutationToken());

    IncrementRequest incrementRequest = new IncrementRequest(kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), id, 1, Optional.empty(), 0, Optional.empty(), null);
    core.send(incrementRequest);

    IncrementResponse incrementResponse = incrementRequest.response().get();
    assertTrue(incrementResponse.status().success());
    assertMutationToken(incrementResponse.mutationToken());
  }

  @Test
  void tokenOnDecrement() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "10".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      kvTimeout, core.context(), collectionIdentifier, env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());
    assertMutationToken(insertResponse.mutationToken());

    DecrementRequest decrementRequest = new DecrementRequest(kvTimeout, core.context(), collectionIdentifier,
      env.retryStrategy(), id, 1, Optional.empty(), 0, Optional.empty(), null);
    core.send(decrementRequest);

    DecrementResponse decrementResponse = decrementRequest.response().get();
    assertTrue(decrementResponse.status().success());
    assertMutationToken(decrementResponse.mutationToken());
  }

  void assertMutationToken(final Optional<MutationToken> token) {
    assertTrue(token.isPresent());
    token.ifPresent(t -> {
      assertEquals(config().bucketname(), t.bucketName());
      assertTrue(t.partitionID() >= 0);
      assertTrue(t.sequenceNumber() > 0);
      assertTrue(t.partitionUUID() != 0);
    });
  }

}
