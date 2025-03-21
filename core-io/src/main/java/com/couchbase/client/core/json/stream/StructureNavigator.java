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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Handles traversal of JSON document structure.
 * Loosely modeled as a state machine (with some cheats).
 */
class StructureNavigator {

  interface State  {
    void accept(JsonToken token);
  }

  /**
   * A "scope" is a special state representing a container node.
   * Useful because Objects and Arrays have different rules
   * for matching values and descending the path tree.
   */
  interface Scope extends State {
    /**
     * Returns the name of the current field or array element.
     */
    String getCurrentName();
  }

  private final Scope objectScope = new ObjectScope();
  private final Scope arrayScope = new ArrayScope();

  private final State readingValue = new ReadingValue();
  private final State fastForwardObject = new FastForwarding(JsonToken.START_OBJECT, JsonToken.END_OBJECT);
  private final State fastForwardArray = new FastForwarding(JsonToken.START_ARRAY, JsonToken.END_ARRAY);

  private enum Mode {
    SCAN, // The current value or one of its descendants might be a match.
    SKIP, // The current value is a dead end.
    CAPTURE, // Found a match, looking for the end.
  }

  private Mode mode = Mode.SCAN;

  /**
   * Hierarchy of field names to scan. Each leaf node holds a callback
   * to invoke when a matching node is found.
   */
  private PathTree pathTree;

  /**
   * Tracks the nested structure of JSON Objects and Arrays.
   */
  private final Deque<Scope> scopeStack = new ArrayDeque<>(singletonList(new RootScope()));
  private State state = scopeStack.getFirst();

  /**
   * The navigator is not aware of token offsets or buffer contents;
   * it asks the parser to deal with those things on its behalf.
   */
  private final JsonStreamParser parser;

  StructureNavigator(JsonStreamParser parser, PathTree pathTree) {
    this.pathTree = requireNonNull(pathTree);
    this.parser = requireNonNull(parser);
  }

  public void accept(JsonToken token) {
    state.accept(token);
  }

  boolean isCapturing() {
    return mode == Mode.CAPTURE;
  }

  private void pushScope(Scope newScope) {
    scopeStack.push(newScope);
    transitionTo(newScope);
  }

  private void popScope() {
    scopeStack.pop();
    transitionTo(currentScope());
  }

  private State transitionTo(State newState) {
    this.state = newState;
    return newState;
  }

  private Scope currentScope() {
    return scopeStack.getFirst(); // like peek, but we want the NoSuchElementException if empty
  }

  /**
   * Climb one level closer to the root of the path tree, or resume scanning
   * if we were previously skipping.
   */
  private void climbPathTree() {
    if (mode == Mode.SKIP) {
      // The path tree is still pointing at the node we failed to match earlier.
      // All we need to do is indicate we're no longer skipping.
      mode = Mode.SCAN;
    } else {
      pathTree = requireNonNull(pathTree.parent(), "expected non-null parent");
    }
    //   System.out.println("  UP TREE -> " + mode + " / " + pathTree);
  }

  /**
   * Navigate one level down from the root of the path tree, or start skipping
   * if there is no route to a potential match.
   */
  private void descendPathTree() {
    if (mode == Mode.SKIP) {
      throw new IllegalStateException("Can't call this when mode is " + Mode.SKIP);
    }

    final String fieldName = currentScope().getCurrentName();
    final PathTree subtree = pathTree.subtree(fieldName);

    if (subtree == null) {
      mode = Mode.SKIP;

    } else {
      pathTree = subtree;

      if (subtree.callback() != null) {
        mode = Mode.CAPTURE;
        parser.beginCapture();
      } else {
        mode = Mode.SCAN;
      }
    }
    // System.out.println("  DOWN TREE -> " + mode + " " + fieldName + " / " + pathTree);
  }

  /**
   * The initial state. Bootstraps the path traversal.
   */
  private class RootScope implements Scope {
    @Override
    public String getCurrentName() {
      return "";
    }

    @Override
    public void accept(JsonToken token) {
      transitionTo(readingValue).accept(token);
    }
  }

  private class ObjectScope implements Scope {
    @Override
    public String getCurrentName() {
      try {
        String currentName = parser.getCurrentName();
        return currentName == null ? "" : currentName;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void accept(JsonToken token) {
      switch (token) {
        case FIELD_NAME:
          transitionTo(readingValue);
          return;

        case END_OBJECT:
          finishValue();
          popScope();
          return;

        default: // Jackson should have caught and reported this
          throw new RuntimeException("Unexpected token: " + token);
      }
    }
  }

  private class ArrayScope implements Scope {
    @Override
    public String getCurrentName() {
      // To support matching specific element we could make array scopes mutable
      // and track the current element index. For now just support matching each element.
      return "-";
    }

    @Override
    public void accept(JsonToken token) {
      switch (token) {
        case END_ARRAY:
          finishValue();
          popScope();
          return;

        default:
          transitionTo(readingValue).accept(token);
      }
    }
  }

  private class ReadingValue implements State {
    @Override
    public void accept(JsonToken token) {
      descendPathTree();

      switch (token) {
        case START_ARRAY:
          readContainer(token, arrayScope, fastForwardArray);
          break;

        case START_OBJECT:
          readContainer(token, objectScope, fastForwardObject);
          return;

        default:
          finishValue();
      }
    }

    private void readContainer(JsonToken startToken, Scope scope, State fastForward) {
      if (mode == Mode.CAPTURE || mode == Mode.SKIP) {
        transitionTo(fastForward).accept(startToken);
      } else {
        pushScope(scope);
      }
    }
  }

  /**
   * In this state the parser only looks for a container's matching end token.
   */
  private class FastForwarding implements State {
    private final JsonToken startToken;
    private final JsonToken endToken;

    private int depth;

    private FastForwarding(JsonToken startToken, JsonToken endToken) {
      this.startToken = requireNonNull(startToken);
      this.endToken = requireNonNull(endToken);
    }

    @Override
    public void accept(JsonToken token) {
      if (token == startToken) {
        depth++;

      } else if (token == endToken) {
        depth--;

        if (depth == 0) {
          finishValue();
        }
      }
    }
  }

  private void finishValue() {
    if (mode == Mode.CAPTURE) {
      mode = Mode.SCAN;
      parser.emitCapturedValue(
        pathTree.jsonPointer(),
        requireNonNull(pathTree.callback(), "missing callback for path tree " + pathTree)
      );
    }
    transitionTo(currentScope()); // return control to the parent Object/Array handler
    climbPathTree();
  }
}
