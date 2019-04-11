package com.couchbase.client.core.json.stream;


import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PathTreeTest {
  @Test
  void parseJsonPointer() throws Exception {
    assertEquals(singletonList(""), PathTree.parseJsonPointer(""));
    assertEquals(Arrays.asList("", ""), PathTree.parseJsonPointer("/"));
    assertEquals(Arrays.asList("", "foo", ""), PathTree.parseJsonPointer("/foo/"));
    assertEquals(Arrays.asList("", "", ""), PathTree.parseJsonPointer("//"));
  }

  @Test
  void nonEmptyPointersMustHaveLeadingSlash() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> PathTree.parseJsonPointer("foo"));
  }

  @Test
  void parents() throws Exception {
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
