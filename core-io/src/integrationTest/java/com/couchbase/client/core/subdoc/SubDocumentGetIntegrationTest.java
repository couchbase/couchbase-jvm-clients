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

package com.couchbase.client.core.subdoc;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.subdoc.*;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.*;
import com.couchbase.client.core.util.CoreIntegrationTest;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

class SubDocumentGetIntegrationTest extends CoreIntegrationTest {

  private static Core core;
  private static CoreEnvironment env;

  @BeforeAll
  static void beforeAll() {
    env = environment().build();
    core = Core.create(env, authenticator(), seedNodes());
    core.openBucket(config().bucketname());
  }

  @AfterAll
  static void afterAll() {
    core.shutdown().block();
    env.shutdown();
  }

  private byte[] insertContent(String id, String in) {
    byte[] content = in.getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      Duration.ofSeconds(1), core.context(), CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(),
      Optional.empty());
    core.send(insertRequest);

    InsertResponse insertResponse = null;
    try {
      insertResponse = insertRequest.response().get();
    } catch (InterruptedException | ExecutionException e) {
      fail("Failed with " + e);
    }
    assertTrue(insertResponse.status().success());

    return content;
  }

  /**
   * Perform subdoc operations and assert the result is the expected exception
   */
  private void checkExpectedFailure(String input, List<SubdocGetRequest.Command> commands, Class<?> expected) {
    String id = UUID.randomUUID().toString();
    insertContent(id, input);

    SubdocGetRequest request = new SubdocGetRequest(Duration.ofSeconds(1), core.context(),
      CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(), id, (byte) 0, commands);
    core.send(request);

    SubdocGetResponse response = null;
    try {
      response = request.response().get();
    } catch (InterruptedException | ExecutionException e) {
      fail("Failed with " + e);
    }
    assertFalse(response.status().success());
    assertEquals(ResponseStatus.SUBDOC_FAILURE, response.status());
    assertTrue(response.error().isPresent());
    SubDocumentException err = response.error().get();
    assertTrue(expected.isInstance(err));
  }

  /**
   * Perform subdoc operations and check the overall result was success
   */
  private SubdocGetResponse checkExpectedSuccess(String input, List<SubdocGetRequest.Command> commands) {
    String id = UUID.randomUUID().toString();
    insertContent(id, input);

    SubdocGetRequest request = new SubdocGetRequest(Duration.ofSeconds(1), core.context(),
      CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(), id, (byte) 0, commands);
    core.send(request);

    SubdocGetResponse response = null;
    try {
      response = request.response().get();
    } catch (InterruptedException | ExecutionException e) {
      fail("Failed with " + e);
    }
    assertTrue(response.status().success());
    assertFalse(response.error().isPresent());
    return response;
  }

  /**
   * Perform a single get subdoc operation and assert the result is the expected exception
   */
  private void singleGetOpCheckExpectedFailure(String input, String path, Class<?> expected) {
    List<SubdocGetRequest.Command> commands = Collections.singletonList(
      new SubdocGetRequest.Command(SubdocCommandType.GET, path, false)
    );

    checkExpectedFailure(input, commands, expected);
  }

  // TODO adding basic tests for DP, but really should port all subdoc tests from old client

  @Test
  void notJson() {
    singleGetOpCheckExpectedFailure("I am not json!", "no_exist", DocumentNotJsonException.class);
  }

  // Fails against real server, passes against mock
  @Ignore
  void notJsonMulti() {
    List<SubdocGetRequest.Command> commands = Arrays.asList(
      new SubdocGetRequest.Command(SubdocCommandType.GET, "foo",  false),
      new SubdocGetRequest.Command(SubdocCommandType.GET, "bar",  false)
    );

    checkExpectedFailure("I am not json!", commands, DocumentNotJsonException.class);
  }

  @Test
  void pathMismatch() {
    singleGetOpCheckExpectedFailure("{\"foo\":\"bar\"}", "foo.bar[0].baz", PathMismatchException.class);
  }

  @Test
  void pathMismatchMulti() {
    List<SubdocGetRequest.Command> commands = Arrays.asList(
      new SubdocGetRequest.Command(SubdocCommandType.GET, "foo",  false),
      new SubdocGetRequest.Command(SubdocCommandType.GET, "foo.bar[0].baz",  false)
    );

    SubdocGetResponse response = checkExpectedSuccess("{\"foo\":\"bar\"}", commands);
    assertTrue(response.values().get(0).status().success());
    assertEquals(SubDocumentOpResponseStatus.PATH_MISMATCH, response.values().get(1).status());
    assertTrue(response.values().get(1).error().get() instanceof PathMismatchException);
  }

  @Test
  void pathNotFound() {
    singleGetOpCheckExpectedFailure("{\"foo\":\"bar\"}", "no_exist", PathNotFoundException.class);
  }

  @Test
  void pathNotFoundMulti() {
    List<SubdocGetRequest.Command> commands = Arrays.asList(
      new SubdocGetRequest.Command(SubdocCommandType.GET, "foo",  false),
      new SubdocGetRequest.Command(SubdocCommandType.GET, "no_exist",  false)
    );

    SubdocGetResponse response = checkExpectedSuccess("{\"foo\":\"bar\"}", commands);
    assertTrue(response.values().get(0).status().success());
    assertEquals(SubDocumentOpResponseStatus.PATH_NOT_FOUND, response.values().get(1).status());
    assertTrue(response.values().get(1).error().get() instanceof PathNotFoundException);
    assertEquals(SubdocCommandType.GET, response.values().get(0).type());
    assertEquals(SubdocCommandType.GET, response.values().get(1).type());
  }

  @Test
  void existsDoesExistSingle() {
    List<SubdocGetRequest.Command> commands = Collections.singletonList(
      new SubdocGetRequest.Command(SubdocCommandType.EXISTS, "foo", false)
    );

    SubdocGetResponse response = checkExpectedSuccess("{\"foo\":\"bar\"}", commands);
    assertTrue(response.values().get(0).status().success());
  }

  @Test
  void existsDoesNotExistSingle() {
    List<SubdocGetRequest.Command> commands = Collections.singletonList(
      new SubdocGetRequest.Command(SubdocCommandType.EXISTS, "cat", false)
    );

    SubdocGetResponse response = checkExpectedSuccess("{\"foo\":\"bar\"}", commands);
    assertFalse(response.values().get(0).status().success());
    assertEquals(SubdocCommandType.EXISTS, response.values().get(0).type());
  }


  @Test
  void existsDoesNotExistMulti() {
    List<SubdocGetRequest.Command> commands = Arrays.asList(
      new SubdocGetRequest.Command(SubdocCommandType.EXISTS, "cat",  false),
      new SubdocGetRequest.Command(SubdocCommandType.GET, "foo",  false)
    );

    SubdocGetResponse response = checkExpectedSuccess("{\"foo\":\"bar\"}", commands);
    assertFalse(response.values().get(0).status().success());
    assertEquals(SubdocCommandType.EXISTS, response.values().get(0).type());
  }
}
