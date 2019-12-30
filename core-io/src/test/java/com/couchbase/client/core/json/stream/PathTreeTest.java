/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.json.stream;

import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PathTreeTest {
  @Test
  void parseJsonPointer() {
    assertEquals(singletonList(""), PathTree.parseJsonPointer(""));
    assertEquals(Arrays.asList("", ""), PathTree.parseJsonPointer("/"));
    assertEquals(Arrays.asList("", "foo", ""), PathTree.parseJsonPointer("/foo/"));
    assertEquals(Arrays.asList("", "", ""), PathTree.parseJsonPointer("//"));
  }

  @Test
  void nonEmptyPointersMustHaveLeadingSlash() {
    assertThrows(InvalidArgumentException.class, () -> PathTree.parseJsonPointer("foo"));
  }

  @Test
  void parents() {
    PathTree root = PathTree.createRoot();
    root.add("/a/b", v -> {
    });
    root.add("/a/c", v -> {
    });

    assertEquals(root, root.subtree("").orElseThrow(AssertionError::new).parent());

    PathTree a = root.subtree("").orElseThrow(AssertionError::new)
      .subtree("a").orElseThrow(AssertionError::new);

    assertEquals(a, a.subtree("b").orElseThrow(AssertionError::new).parent());
    assertEquals(a, a.subtree("c").orElseThrow(AssertionError::new).parent());
  }
}
