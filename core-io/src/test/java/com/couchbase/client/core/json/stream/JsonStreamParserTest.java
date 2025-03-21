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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonStreamParserTest {

  @Test
  void exampleUsage() {
    byte[] json = "{'name':'Jon','pets':[{'name':'Odie'},{'name':'Garfield'}],'blob':{'magicWord':'xyzzy'}}"
      .replace("'", "\"")
      .getBytes(UTF_8);

    List<String> matches = new ArrayList<>();

    JsonStreamParser.Builder builder = JsonStreamParser.builder()
      .doOnValue("/name", v -> matches.add("Hello " + v.readString()))
      .doOnValue("/pets/-/name", v -> matches.add("I see you have a pet named " + v.readString()))
      .doOnValue("/blob", v -> matches.add("Here's a blob of JSON: " + new String(v.bytes(), UTF_8)));

    try (JsonStreamParser parser = builder.build()) {
      int half = json.length / 2;
      parser.feed(json, 0, half);
      assertEquals(
        Arrays.asList(
          "Hello Jon",
          "I see you have a pet named Odie"
        ),
        matches
      );

      matches.clear();
      parser.feed(json, half, json.length - half);
      assertEquals(
        Arrays.asList(
          "I see you have a pet named Garfield",
          "Here's a blob of JSON: {\"magicWord\":\"xyzzy\"}"
        ),
        matches
      );
    }
  }

  @Test
  void canFeedFromByteBuffer() {
    byte[] json = "{'magicWord':'xyzzy'}"
      .replace("'", "\"")
      .getBytes(UTF_8);

    List<String> matches = new ArrayList<>();

    JsonStreamParser.Builder builder = JsonStreamParser.builder()
      .doOnValue("/magicWord", v -> matches.add(v.readString()));

    try (JsonStreamParser parser = builder.build()) {
      int half = json.length / 2;
      parser.feed(ByteBuffer.wrap(json, 0, half));
      parser.feed(ByteBuffer.wrap(json, half, json.length - half));
      parser.endOfInput();
    }

    assertEquals(singletonList("xyzzy"), matches);
  }

  @Test
  void jsonPointerExamples() throws Exception {
    String json = "{\n" +
      "  'foo': ['bar', 'baz'],\n" +
      "  '': 0,\n" +
      "  'a/b': 1,\n" +
      "  'c%d': 2,\n" +
      "  'e^f': 3,\n" +
      "  'g|h': 4,\n" +
      "  'i\\\\j': 5,\n" +
      "  'k\\'l': 6,\n" +
      "  ' ': 7,\n" +
      "  'm~n': 8\n" +
      "}";

    new ResultChecker(json)
      //""           // the whole document
      .expect("/foo", "['bar', 'baz']")
      //.expect("/foo/0", "'bar'")
      .expect("/", "0")
      .expect("/a~1b", "1")
      .expect("/c%d", "2")
      .expect("/e^f", "3")
      .expect("/g|h", "4")
      .expect("/i\\j", "5")
      .expect("/k\"l", "6")
      .expect("/ ", "7")
      .expect("/m~0n", "8")
      .check();
  }

  @Test
  void surroundingWhitespaceOkay() throws Exception {
    String json = " {} ";

    new ResultChecker(json)
      .expect("", "{}")
      .check();
  }

  @Test
  void process() throws Exception {
    String json = "{" +
      "'ignoreScalar':false," +
      "'ignoreArray':[[1,2,3],[{}]]," +
      "'ignoreObject':{'a':{},'b':[{}]}," +
      "'greeting':'hello'," +
      "'animal' : {'name' :  'fi\\'do' ,'age':5}," +
      "'numbers' : [1, 'two' , 'π']," +
      "'magicWords': ['xyzzy' , 'abracadabra']," +
      "'null':null" +
      "}";

    new ResultChecker(json)
      .expect("/greeting", "'hello'")
      .expect("/animal/name", "'fi\\'do'")
      .expect("/animal/age", "5")
      .expect("/numbers/-", "1", "'two'", "'π'")
      .expect("/magicWords", "['xyzzy' , 'abracadabra']")
      .expect("/null", "null")
      .check();
  }

  @Test
  void matchElementsOfRootArray() throws Exception {
    String json = "[1,{'foo' : true},3]";

    new ResultChecker(json)
      .expect("/-", "1", "{'foo' : true}", "3")
      .check();
  }

  @Test
  void matchRootObject() throws Exception {
    String json = "{'greeting':'hello'}";

    new ResultChecker(json)
      .expect("", "{'greeting':'hello'}")
      .check();
  }

  @Test
  void matchRootArray() throws Exception {
    String json = "[1, 2 ,3]";

    new ResultChecker(json)
      .expect("", "[1, 2 ,3]")
      .check();
  }

  @Test
  void matchRootScalar() throws Exception {
    String json = "true";

    new ResultChecker(json)
      .expect("", "true")
      .check();
  }

  @Test
  void matchEmptyName() throws Exception {
    String json = "{'':'hello'}";

    new ResultChecker(json)
      .expect("/", "'hello'")
      .check();
  }

  @Test
  void containerTypeMismatchIsNotError() throws Exception {
    String json = "{'greeting':'hello','colors':['red','green','blue']}";

    new ResultChecker(json)
      .expect("/greeting/-")
      .expect("/colors/foo")
      .check();
  }

  @Test
  void hyphenIsValidObjectFieldName() throws Exception {
    String json = "{'foo':{'-':'bar'}}";
    new ResultChecker(json)
      .expect("/foo/-", "'bar'")
      .check();
  }

  @Test
  void canReadMultipleDocuments() throws Exception {
    String json = "{'color':'red'}{'color':'green'}";
    new ResultChecker(json)
      .expect("/color", "'red'", "'green'")
      .check();
  }

  @Test
  void canDescendThroughArrayWildcard() throws Exception {
    String json = "{'colors':[{'r':255,'g':0,'b':0},{'r':0,'g':255,'b':0}]}";
    new ResultChecker(json)
      .expect("/colors/-/g", "0", "255") // I can't believe this works :-)
      .check();
  }

  @Test
  void cannotReconfigureAfterBuilding() throws Exception {
    JsonStreamParser.Builder builder = JsonStreamParser.builder()
      .doOnValue("/foo", v -> {
      });


    builder.build().close();

    assertThrows(IllegalStateException.class, () ->
      // bad because the path tree is mutable and shared by all parsers from this builder
      builder.doOnValue("/bar", v -> {
      }));
  }

  private static class ResultChecker {
    private static class ListenerCheck {
      private final String jsonPointer;
      private final List<String> expected;
      private final List<String> actual = new ArrayList<>();

      public ListenerCheck(String jsonPointer, List<String> expected) {
        this.jsonPointer = jsonPointer;
        this.expected = expected;
      }

      void addActual(String value) {
        actual.add(value);
      }

      void checkResult() {
        assertEquals(expected, actual, jsonPointer);
      }
    }

    private final List<ListenerCheck> checks = new ArrayList<>();
    private final JsonStreamParser.Builder builder = JsonStreamParser.builder();
    private final byte[] json;

    ResultChecker(String json) {
      this.json = normalizeQuotes(json).getBytes(UTF_8);
    }

    ResultChecker expect(String jsonPointer, String... expectedValues) {
      List<String> expected = Arrays.stream(expectedValues)
        .map(ResultChecker::normalizeQuotes)
        .collect(toList());

      ListenerCheck check = new ListenerCheck(jsonPointer, expected);
      builder.doOnValue(jsonPointer, value -> check.addActual(new String(value.bytes(), UTF_8)));
      checks.add(check);
      return this;
    }

    private static String normalizeQuotes(String s) {
      return s.replace("'", "\"");
    }

    void check() throws IOException {
      checkWithChunkSizeAndStreamWindow(Integer.MAX_VALUE);

      for (int i = 1; i <= min(32, json.length); i++) {
        checkWithChunkSizeAndStreamWindow(i);
      }
    }

    void checkWithChunkSizeAndStreamWindow(final int chunkSize) throws IOException {
      checks.forEach(c -> c.actual.clear()); // reset

      try (JsonStreamParser parser = builder.build()) {
        Buffer buf = new Buffer();
        buf.writeBytes(json);

        parser.feed(new byte[0], 0, 0); // make sure empty chunk doesn't break anything

        while (buf.isReadable()) {
          Buffer chunk = new Buffer();
          chunk.writeBytes(buf, min(chunkSize, buf.readableBytes()));
          parser.feed(chunk.array(), chunk.readerIndex(), chunk.readableBytes());
        }
        parser.endOfInput();
      }

      checks.forEach(ListenerCheck::checkResult);
    }
  }
}
