/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HeadInterceptInputStreamTest {

  private static InputStream bytes(String s) {
    return new ByteArrayInputStream(s.getBytes(UTF_8));
  }

  /**
   * Returns at most one byte per read, like a slow network connection.
   */
  private static InputStream trickle(InputStream in) {
    return new FilterInputStream(in) {
      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        return super.read(b, off, Math.min(len, 1));
      }
    };
  }

  private static String readAll(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    int c;
    while ((c = in.read()) != -1) {
      sb.append((char) c);
    }
    return sb.toString();
  }

  @Test
  void remembersHeadOfWhatWasRead() throws IOException {
    HeadInterceptInputStream in = new HeadInterceptInputStream(bytes("hello world"), 5);

    assertEquals("hello world", readAll(in));
    assertEquals("hello", in.getHeadAsString());
  }

  @Test
  void headIsOnlyWhatWasReadSoFar() throws IOException {
    HeadInterceptInputStream in = new HeadInterceptInputStream(trickle(bytes("hello world")), 5);

    byte[] buffer = new byte[10];
    assertEquals(1, in.read(buffer)); // the reader gives up after one byte, like a parser that fails on the first byte

    assertEquals("h", in.getHeadAsString());
  }

  @Test
  void fillHeadReadsTheRestOfTheHead() throws IOException {
    HeadInterceptInputStream in = new HeadInterceptInputStream(trickle(bytes("hello world")), 5);
    in.read(new byte[10]);

    in.fillHead();

    assertEquals("hello", in.getHeadAsString());
  }

  @Test
  void fillHeadDoesNotReadBeyondTheHead() throws IOException {
    HeadInterceptInputStream in = new HeadInterceptInputStream(bytes("hello world"), 5);

    in.fillHead();

    assertEquals("hello", in.getHeadAsString());
    assertEquals(" world", readAll(in));
  }

  @Test
  void fillHeadStopsAtEndOfStream() throws IOException {
    HeadInterceptInputStream in = new HeadInterceptInputStream(trickle(bytes("hi")), 5);

    in.fillHead();

    assertEquals("hi", in.getHeadAsString());
  }

  @Test
  void fillHeadDoesNothingIfHeadIsAlreadyFull() throws IOException {
    HeadInterceptInputStream in = new HeadInterceptInputStream(bytes("hello world"), 5);
    in.read(new byte[7]);

    in.fillHead();

    assertEquals("hello", in.getHeadAsString());
    assertEquals("orld", readAll(in));
  }

  @Test
  void zeroLimitRemembersNothing() throws IOException {
    HeadInterceptInputStream in = new HeadInterceptInputStream(bytes("hello"), 0);

    in.fillHead();

    assertEquals("", in.getHeadAsString());
    assertEquals("hello", readAll(in));
  }

  @Test
  void remembersFirstReadFailure() throws Exception {
    IOException broken = new IOException("unexpected end of stream");
    InputStream failing = new InputStream() {
      int calls;

      @Override
      public int read() throws IOException {
        if (calls++ == 0) {
          return 'x';
        }
        throw calls == 2 ? broken : new IOException("a later failure");
      }
    };

    HeadInterceptInputStream in = new HeadInterceptInputStream(failing, 100);
    assertNull(in.readFailure());
    assertEquals('x', in.read());
    assertNull(in.readFailure());

    assertSame(broken, assertThrows(IOException.class, in::read));
    assertThrows(IOException.class, () -> in.read(new byte[10], 0, 10));
    assertSame(broken, in.readFailure()); // the first
  }

  @Test
  void contentProblemsAreNotReadFailures() throws Exception {
    // A reader (for example, a JSON parser) that rejects what it read doesn't make it a read failure.
    HeadInterceptInputStream in = new HeadInterceptInputStream(new ByteArrayInputStream("not json".getBytes(UTF_8)), 100);
    while (in.read() != -1) {
      // read it all
    }
    assertNull(in.readFailure());
  }
}
