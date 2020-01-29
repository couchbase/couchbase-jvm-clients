/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.java.kv.projections;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.projections.JsonPathParser;
import com.couchbase.client.core.projections.PathArray;
import com.couchbase.client.core.projections.PathElement;
import com.couchbase.client.core.projections.PathObjectOrField;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Helper functions to aid with parsing get-with-projections calls.
 *
 * Largely a port of Scala's ProjectionsApplier.  It cannot be moved into core as it needs to handle both Java and
 * Scala's JsonObject & JsonArray.
 *
 * @since 3.0.0
 */
@Stability.Internal
public class ProjectionsApplier {

  private ProjectionsApplier() { }

  static Object parseContent(byte[] content) {
    byte first = content[0];
    switch (first) {
      case '{':
        return JsonObject.fromJson(content);
      case '[':
        return JsonArray.fromJson(content);
      case '"': {
        String str = new String(content, StandardCharsets.UTF_8);
        return str.substring(1, str.length() - 1);
      }
      case 't':
        return Boolean.TRUE;
      case 'f':
        return Boolean.FALSE;
      case 'n':
        return null;
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9': {
        String str = new String(content, StandardCharsets.UTF_8);
        if (str.contains(".")) return Double.parseDouble(str);
        else return Long.parseLong(str);
      }
      default:
        return new String(content, StandardCharsets.UTF_8);
    }
  }

  public static JsonObject parse(JsonObject in, String path, byte[] contentRaw) {
    List<PathElement> parsed = JsonPathParser.parse(path);
    Object content = parseContent(contentRaw);
    parseRecursive(in, parsed, content);
    return in;
  }

  /**
   * Will follow `path`, constructing JSON as it does into `out`, and inserting `content` at the leaf
   *
   * @param out must be either a JsonObject or JsonArray, and the JSON will be constructed into it
   */
  static void parseRecursive(final Object out,
                             final List<PathElement> path,
                       final Object content) {
    if (path.isEmpty()) {
      // Recursion is done
      return;
    }

    if (path.size() == 1) {
      // At leaf of path
      PathElement leaf = path.get(0);

      if (leaf instanceof PathArray) {
        PathArray v = (PathArray) leaf;
        JsonArray toInsert = JsonArray.create().add(content);

        if (out instanceof JsonObject) {
          ((JsonObject) out).put(v.str(), toInsert);
        }
        else {
          ((JsonArray) out).add(toInsert);
        }
      }
      else {
        PathObjectOrField v = (PathObjectOrField) leaf;

        if (out instanceof JsonObject) {
          ((JsonObject) out).put(v.str(), content);
        }
        else {
          JsonObject toInsert = JsonObject.create();
          toInsert.put(v.str(), content);
          ((JsonArray) out).add(toInsert);
        }
      }
    }
    else {
      // Non-leaf
      PathElement next = path.get(0);
      List<PathElement> remaining = path.subList(1, path.size());

      if (next instanceof PathArray) {
        PathArray v = (PathArray) next;
        JsonArray toInsert = JsonArray.create();

        if (out instanceof JsonObject) {
          ((JsonObject) out).put(v.str(), toInsert);
          parseRecursive(toInsert, remaining, content);
        }
        else {
          ((JsonArray) out).add(toInsert);
          parseRecursive(out, remaining, content);
        }
      }
      else {
        PathObjectOrField v = (PathObjectOrField) next;

        if (out instanceof JsonObject) {
          JsonObject createIn = ((JsonObject) out).getObject(v.str());
          if (createIn == null) {
            createIn = JsonObject.create();
          }
          ((JsonObject) out).put(v.str(), createIn);
          parseRecursive(createIn, remaining, content);
        }
        else {
          JsonObject toCreate = JsonObject.create();
          JsonObject nextToCreate = JsonObject.create();
          toCreate.put(v.str(), nextToCreate);
          ((JsonArray) out).add(toCreate);
          parseRecursive(nextToCreate, remaining, content);
        }
      }
    }
  }
}


