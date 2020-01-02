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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GetAccessorTest {

  @Test
  void projectNonRecursive() {
    Map<String, String> paths = new HashMap<>();
    paths.put("foo", "\"bar\"");
    paths.put("created", "true");
    paths.put("number", "42");

    byte[] result = GetAccessor.projectRecursive(response(paths));

    JsonObject expected = JsonObject.create()
      .put("foo", "bar")
      .put("created", true)
      .put("number", 42);

    assertEquals(expected, JsonObject.fromJson(result));
  }

  @Test
  void ignoresEmptyField() {
    Map<String, String> paths = new HashMap<>();
    paths.put("", "\"bar\"");
    paths.put("created", "true");

    byte[] result = GetAccessor.projectRecursive(response(paths));

    JsonObject expected = JsonObject.create()
      .put("created", true);

    assertEquals(expected, JsonObject.fromJson(result));
  }

  @Test
  void ignoresExpirationMacro() {
    Map<String, String> paths = new HashMap<>();
    paths.put(GetAccessor.EXPIRATION_MACRO, "\"bar\"");
    paths.put("created", "true");

    byte[] result = GetAccessor.projectRecursive(response(paths));

    JsonObject expected = JsonObject.create()
      .put("created", true);

    assertEquals(expected, JsonObject.fromJson(result));
  }

  @Test
  void nestedObjects() {
    Map<String, String> paths = new HashMap<>();
    paths.put("l1.l2.k1", "\"v1\"");
    paths.put("l1.k2", "\"v2\"");
    paths.put("k3", "true");

    byte[] result = GetAccessor.projectRecursive(response(paths));

    JsonObject expected = JsonObject.create()
      .put("k3", true)
      .put("l1", JsonObject.create()
        .put("k2", "v2")
        .put("l2", JsonObject.create()
          .put("k1", "v1")
        )
      );

     assertEquals(expected, JsonObject.fromJson(result));
  }

  /**
   * Helper method to build the response from a list of paths and values.
   *
   * @param paths the paths and values.
   * @return a created response.
   */
  private SubdocGetResponse response(final Map<String, String> paths) {
    List<SubDocumentField> values = paths
      .entrySet()
            .stream()
            .map(e ->
                    new SubDocumentField(
                            SubDocumentOpResponseStatus.SUCCESS,
                            Optional.empty(),
                            e.getValue().getBytes(UTF_8),
                            e.getKey(),
                            SubdocCommandType.GET
                    )
            )
      .collect(Collectors.toList());
    return new SubdocGetResponse(ResponseStatus.SUCCESS, Optional.empty(), values.toArray(new SubDocumentField[values.size()]), 0);
  }
}
