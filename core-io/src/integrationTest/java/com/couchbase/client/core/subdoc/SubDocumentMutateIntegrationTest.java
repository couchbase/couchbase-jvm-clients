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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

class SubDocumentMutateIntegrationTest extends CoreIntegrationTest {

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment().build();
    core = Core.create(env, authenticator(), seedNodes());
    core.openBucket(config().bucketname());
  }

  @AfterEach
  void afterEach() {
    core.shutdown().block();
    env.shutdown();
  }

  private byte[] insertContent(String id, String in) {
    byte[] content = in.getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
            Duration.ofSeconds(1), core.context(), CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(),
      Optional.empty(), null);
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


  // TODO add macro expansion support
//  @Ignore("requires macro expansion support")
//  @Test
//  void xattrUnknownMacroError() throws Exception {
//    testSingleGetChildFailure("hello, world", "$no_exist", XattrUnknownMacroException.class);
//  }

}
