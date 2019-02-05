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
import com.couchbase.client.core.msg.kv.*;
import com.couchbase.client.core.util.CoreIntegrationTest;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class MutationTokenIntegrationTest extends CoreIntegrationTest {

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment().mutationTokensEnabled(true).build();
    core = Core.create(env);
    core.openBucket(config().bucketname()).block();
  }

  @AfterEach
  void afterEach() {
    core.shutdown().block();
    env.shutdown(Duration.ofSeconds(1));
  }

  @Test
  void tokenOnAppend() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello".getBytes(CharsetUtil.UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    AppendRequest appendRequest = new AppendRequest(Duration.ofSeconds(1), core.context(), config().bucketname(),
      env.retryStrategy(), id, null, ", world".getBytes(CharsetUtil.UTF_8), upsertResponse.cas(), Optional.empty());
    core.send(appendRequest);

    AppendResponse appendResponse = appendRequest.response().get();
    assertTrue(appendResponse.status().success());
    assertMutationToken(appendResponse.mutationToken());
  }

  @Test
  void tokenOnPrepend() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello".getBytes(CharsetUtil.UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    PrependRequest prependRequest = new PrependRequest(Duration.ofSeconds(1), core.context(), config().bucketname(),
      env.retryStrategy(), id, null, ", world".getBytes(CharsetUtil.UTF_8), upsertResponse.cas(), Optional.empty());
    core.send(prependRequest);

    PrependResponse prependResponse = prependRequest.response().get();
    assertTrue(prependResponse.status().success());
    assertMutationToken(prependResponse.mutationToken());
  }

  @Test
  void tokenOnSubdocMutate() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "{}".getBytes(CharsetUtil.UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    List<SubdocMutateRequest.Command> commands = new ArrayList<>();
    commands.add(new SubdocMutateRequest.Command(
      SubdocCommandType.DICT_ADD,
      "foo",
      "\"bar\"".getBytes(CharsetUtil.UTF_8),
      true,
      false,
            false)
    );
    SubdocMutateRequest subdocMutateRequest = new SubdocMutateRequest(Duration.ofSeconds(1), core.context(),
      config().bucketname(), env.retryStrategy(), id, null, false, commands, 0, Optional.empty());
    core.send(subdocMutateRequest);

    SubdocMutateResponse subdocMutateResponse = subdocMutateRequest.response().get();
    assertTrue(subdocMutateResponse.status().success());
    assertMutationToken(subdocMutateResponse.mutationToken());
  }

  @Test
  void tokenOnUpsert() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(upsertRequest);

    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());
  }

  @Test
  void tokenOnReplace() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    ReplaceRequest replaceRequest = new ReplaceRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), upsertResponse.cas(), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(replaceRequest);

    ReplaceResponse replaceResponse = replaceRequest.response().get();
    assertTrue(replaceResponse.status().success());
    assertMutationToken(replaceResponse.mutationToken());
  }

  @Test
  void tokenOnRemove() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(upsertRequest);
    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertMutationToken(upsertResponse.mutationToken());

    RemoveRequest removeRequest = new RemoveRequest(id, null, upsertResponse.cas(),
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(removeRequest);

    RemoveResponse removeResponse = removeRequest.response().get();
    assertTrue(removeResponse.status().success());
    assertMutationToken(removeResponse.mutationToken());
  }

  @Test
  void tokenOnInsert() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());
    assertMutationToken(insertResponse.mutationToken());
  }

  @Test
  void tokenOnIncrement() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "1".getBytes(CharsetUtil.UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());
    assertMutationToken(insertResponse.mutationToken());

    IncrementRequest incrementRequest = new IncrementRequest(Duration.ofSeconds(1), core.context(), config().bucketname(),
      env.retryStrategy(), id, null, 1, Optional.empty(), 0, Optional.empty());
    core.send(incrementRequest);

    IncrementResponse incrementResponse = incrementRequest.response().get();
    assertTrue(incrementResponse.status().success());
    assertMutationToken(incrementResponse.mutationToken());
  }

  @Test
  void tokenOnDecrement() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "10".getBytes(CharsetUtil.UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());
    assertMutationToken(insertResponse.mutationToken());

    DecrementRequest decrementRequest = new DecrementRequest(Duration.ofSeconds(1), core.context(), config().bucketname(),
      env.retryStrategy(), id, null, 1, Optional.empty(), 0, Optional.empty());
    core.send(decrementRequest);

    DecrementResponse decrementResponse = decrementRequest.response().get();
    assertTrue(decrementResponse.status().success());
    assertMutationToken(decrementResponse.mutationToken());
  }

  void assertMutationToken(final Optional<MutationToken> token) {
    assertTrue(token.isPresent());
    token.ifPresent(t -> {
      assertEquals(config().bucketname(), t.bucket());
      assertTrue(t.vbucketID() >= 0);
      assertTrue(t.sequenceNumber() > 0);
      assertTrue(t.vbucketUUID() != 0);
    });
  }

}
