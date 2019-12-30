/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.core.json.stream;

import com.couchbase.client.core.error.InvalidArgumentException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A tree of field names. Leaf nodes have callback handlers.
 */
class PathTree {
  private String jsonPointer;
  private Optional<Consumer<MatchedValue>> callback = Optional.empty();

  private final Map<String, PathTree> children = new HashMap<>();
  private final String name;
  private final PathTree parent;

  private PathTree(PathTree parent, String name) {
    this.parent = parent;
    this.name = requireNonNull(name);
  }

  static PathTree createRoot() {
    return new PathTree(null, "$ROOT");
  }

  Optional<Consumer<MatchedValue>> callback() {
    return callback;
  }

  String jsonPointer() {
    return jsonPointer;
  }

  PathTree parent() {
    return parent;
  }

  Optional<PathTree> subtree(String name) {
    return Optional.ofNullable(children.get(name));
  }

  private PathTree getOrCreateSubtree(String name) {
    return children.computeIfAbsent(name, ignoreKey -> new PathTree(this, name));
  }

  static List<String> parseJsonPointer(String jsonPointer) {
    if (!jsonPointer.startsWith("/") && !jsonPointer.isEmpty()) {
      throw InvalidArgumentException.fromMessage("JSON pointer must be empty or start with forward slash (/) but got \"" + jsonPointer + "\"");
    }

    final int HONOR_ALL_DELIMITERS = -1;
    return Arrays.stream(jsonPointer.split("/", HONOR_ALL_DELIMITERS))
      .map(s -> s.replace("~1", "/").replace("~0", "~")) // decode escape sequences
      .collect(toList());
  }

  /**
   * @throws IllegalStateException if there is already a callback at the given path,
   *                               or if adding the path would violate the constraint that
   *                               internal nodes may not have callbacks.
   */
  void add(String jsonPointer, Consumer<MatchedValue> callback) {
    add(parseJsonPointer(jsonPointer), jsonPointer, callback);
  }

  private void add(List<String> path, String jsonPointer, Consumer<MatchedValue> callback) {
    requireNonNull(callback);
    requireNonNull(jsonPointer);

    PathTree tree = this;
    for (String p : path) {
      tree = tree.getOrCreateSubtree(p);

      if (tree.callback.isPresent()) {
        // only leaf nodes may have a callback, and only one callback per node
        throw new IllegalStateException("Already have a callback for path " + path + " or one of its ancestors.");
      }
    }

    if (!tree.children.isEmpty()) {
      throw new IllegalStateException("Already have a callback for a descendant of path " + path);
    }

    tree.jsonPointer = jsonPointer;
    tree.callback = Optional.of(callback);
  }

  @Override
  public String toString() {
    return "\"" + name + "\"" + (children.values().isEmpty() ? "!" : (":" + children.values()));
  }
}
