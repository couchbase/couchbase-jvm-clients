/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.core.projections;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ProjectionsApplierTest {
  @Test
  void projectNonRecursive() {
    check(
        mapOf(
            "foo", "\"bar\"",
            "created", "true",
            "number", "42"),
        mapOf(
            "foo", "bar",
            "created", true,
            "number", 42)
    );
  }

  @Test
  void ignoresEmptyField() {
    check(
        mapOf(
            "", "\"bar\"",
            "created", "true"),
        mapOf(
            "created", true)
    );
  }

  @Test
  void ignoresExpirationMacro() {
    check(
        mapOf(
            "$document.exptime", "\"bar\"",
            "created", "true"),
        mapOf(
            "created", true)
    );
  }

  @Test
  void nestedObjects() {
    check(
        mapOf(
            "l1.l2.k1", "\"v1\"",
            "l1.k2", "\"v2\"",
            "k3", "true"),
        mapOf(
            "k3", true,
            "l1", mapOf(
                "k2", "v2",
                "l2", mapOf(
                    "k1", "v1"
                )
            ))
    );
  }

  private static void check(Map<String, String> subdocResponseFields,
                            Map<String, Object> expectedJson) {
    byte[] result = ProjectionsApplier.reconstructDocument(response(subdocResponseFields));

    assertEquals(
        Mapper.convertValue(expectedJson, JsonNode.class),
        Mapper.decodeIntoTree(result));
  }

  /**
   * Helper method to build the response from a list of paths and values.
   *
   * @param paths the paths and values.
   * @return a created response.
   */
  private static SubdocGetResponse response(final Map<String, String> paths) {
    SubDocumentField[] values = paths
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
        .toArray(SubDocumentField[]::new);

    return new SubdocGetResponse(ResponseStatus.SUCCESS, Optional.empty(), values, 0, false);
  }
}
