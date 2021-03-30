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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ContainerNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;

import java.util.List;
import java.util.Map;

/**
 * Helper functions to aid with parsing get-with-projections calls.
 * Largely a port of Scala's ProjectionsApplier.
 *
 * @since 2.1.2
 */
public class ProjectionsApplier {

  private static final String MACRO_PREFIX = "$document";

  private ProjectionsApplier() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns the bytes of a JSON Object created by projecting the
   * subdoc response fields into the object structure.
   */
  public static byte[] reconstructDocument(SubdocGetResponse subdocGetResponse) {
    ObjectNode result = Mapper.createObjectNode();

    for (SubDocumentField field : subdocGetResponse.values()) {
      if (field == null
          || field.status() != SubDocumentOpResponseStatus.SUCCESS
          || field.path().isEmpty()
          || field.path().startsWith(MACRO_PREFIX)) {
        continue;
      }

      insert(result, field);
    }

    return Mapper.encodeAsBytes(result);
  }

  public static byte[] reconstructDocument(Map<String, byte[]> pathToValue) {
    ObjectNode result = Mapper.createObjectNode();
    pathToValue.forEach((path, value) -> insert(result, path, value));
    return Mapper.encodeAsBytes(result);
  }

  private static void insert(ObjectNode document, SubDocumentField field) {
    insert(document, field.path(), field.value());
  }

  private static void insert(ObjectNode document, String path, byte[] value) {
    List<PathElement> parsedPath = JsonPathParser.parse(path);
    JsonNode content = Mapper.decodeIntoTree(value);
    insertRecursive(document, parsedPath, content);
  }

  /**
   * Will follow `path`, constructing JSON as it does into `out`, and inserting `content` at the leaf
   *
   * @param out must be either a ObjectNode or ArrayNode, and the JSON will be constructed into it
   */
  private static void insertRecursive(final ContainerNode<?> out,
                                      final List<PathElement> path,
                                      final JsonNode content) {
    if (path.isEmpty()) {
      // Recursion is done
      return;
    }

    if (path.size() == 1) {
      // At leaf of path
      PathElement leaf = path.get(0);

      if (leaf instanceof PathArray) {
        PathArray v = (PathArray) leaf;

        ArrayNode toInsert = out.arrayNode().add(content);

        if (out.isObject()) {
          ((ObjectNode) out).set(v.str(), toInsert);
        } else {
          ((ArrayNode) out).add(toInsert);
        }
      } else {
        PathObjectOrField v = (PathObjectOrField) leaf;

        if (out.isObject()) {
          ((ObjectNode) out).set(v.str(), content);
        } else {
          ObjectNode toInsert = out.objectNode();
          toInsert.set(v.str(), content);
          ((ArrayNode) out).add(toInsert);
        }
      }
    } else {
      // Non-leaf
      PathElement next = path.get(0);
      List<PathElement> remaining = path.subList(1, path.size());

      if (next instanceof PathArray) {
        PathArray v = (PathArray) next;
        ArrayNode toInsert = out.arrayNode();

        if (out.isObject()) {
          ((ObjectNode) out).set(v.str(), toInsert);
          insertRecursive(toInsert, remaining, content);
        } else {
          ((ArrayNode) out).add(toInsert);
          insertRecursive(out, remaining, content);
        }
      } else {
        PathObjectOrField v = (PathObjectOrField) next;

        if (out.isObject()) {
          ObjectNode createIn = (ObjectNode) out.get(v.str());
          if (createIn == null) {
            createIn = out.objectNode();
          }
          ((ObjectNode) out).set(v.str(), createIn);
          insertRecursive(createIn, remaining, content);
        } else {
          ObjectNode toCreate = out.objectNode();
          ObjectNode nextToCreate = out.objectNode();
          toCreate.set(v.str(), nextToCreate);
          ((ArrayNode) out).add(toCreate);
          insertRecursive(nextToCreate, remaining, content);
        }
      }
    }
  }
}


