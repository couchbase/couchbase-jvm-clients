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

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.RequestContext;
import okhttp3.Connection;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.ForwardingSource;
import okio.Okio;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
/**
 * Captures the HTTP traffic of the services in {@link IoConfig#servicesToCapture()}, like the Netty endpoints'
 * {@code TrafficCaptureHandler}: publishes a {@link WriteTrafficCapturedEvent} for each request as it's sent
 * (request line, headers, and body), and {@link ReadTrafficCapturedEvent}s for its response as it's received
 * (status line and headers, then each chunk of the body as the caller reads it).
 * <p>
 * Request and status lines and headers are captured as text. A body is captured as text if its content type is
 * textual (see {@link #isTextual}); a character split between two chunks is captured whole, with the later chunk.
 * Otherwise, it's captured as a hex dump, in the same format as the Netty endpoints'.
 * <p>
 * Unlike the Netty endpoints, which capture the bytes on the wire (as hex dumps), this captures the HTTP messages
 * as OkHttp sends and receives them, mostly as text: OkHttp's own framing (like chunked encoding) isn't included,
 * and the Authorization header's value is redacted. (Capturing the bytes inside a TLS connection would mean wrapping
 * OkHttp's SSLSocket, which OkHttp depends on the exact type of.)
 * <p>
 * Added as a network interceptor, so it sees each request as sent on the connection.
 * <p>
 * Thread-safe.
 */
@NullMarked
final class TrafficCaptureInterceptor implements Interceptor {
  private final Set<ServiceType> servicesToCapture;

  TrafficCaptureInterceptor(Set<ServiceType> servicesToCapture) {
    this.servicesToCapture = servicesToCapture.isEmpty()
      ? EnumSet.noneOf(ServiceType.class)
      : EnumSet.copyOf(servicesToCapture);
  }

  @Override
  public Response intercept(Chain chain) throws IOException {
    Request request = chain.request();
    RequestContext requestContext = request.tag(RequestContext.class);
    if (requestContext == null || !servicesToCapture.contains(serviceType(requestContext))) {
      return chain.proceed(request);
    }

    IoContext ioContext = ioContext(requestContext, chain.connection());
    publish(requestContext, new WriteTrafficCapturedEvent(ioContext, describe(request)));

    Response response = chain.proceed(request);
    publish(requestContext, new ReadTrafficCapturedEvent(ioContext, describe(response)));

    ResponseBody body = response.body();
    boolean textual = isTextual(response.header("Content-Type"));
    Utf8ChunkDecoder decoder = new Utf8ChunkDecoder();
    ResponseBody captured = ResponseBody.create(
      Okio.buffer(new ForwardingSource(body.source()) {
        private long bodyOffset; // of the next chunk, for the hex dumps

        @Override
        public long read(Buffer sink, long byteCount) throws IOException {
          long read = super.read(sink, byteCount);
          String captured;
          if (read > 0) {
            Buffer chunk = new Buffer();
            sink.copyTo(chunk, sink.size() - read, read);
            byte[] bytes = chunk.readByteArray();
            if (textual) {
              long textOffset = decoder.decodedBytes();
              String text = decoder.decode(bytes);
              captured = text.isEmpty() ? "" : framedText(text, textOffset, decoder.decodedBytes() - textOffset);
            } else {
              captured = hexDump(bytes, bodyOffset);
            }
            bodyOffset += read;
          } else if (read == -1 && textual) {
            long textOffset = decoder.decodedBytes();
            String text = decoder.flush();
            captured = text.isEmpty() ? "" : framedText(text, textOffset, decoder.decodedBytes() - textOffset);
          } else {
            captured = "";
          }
          if (!captured.isEmpty()) {
            publish(requestContext, new ReadTrafficCapturedEvent(ioContext, captured));
          }
          return read;
        }
      }),
      body.contentType(),
      body.contentLength()
    );
    return response.newBuilder().body(captured).build();
  }

  private static @Nullable ServiceType serviceType(RequestContext requestContext) {
    try {
      return requestContext.request().serviceType();
    } catch (RuntimeException e) {
      return null; // for example, a request with no service type
    }
  }

  private static IoContext ioContext(RequestContext requestContext, @Nullable Connection connection) {
    Socket socket = connection == null ? null : connection.socket();
    return new IoContext(
      requestContext,
      socket == null ? null : socket.getLocalSocketAddress(),
      socket == null ? null : socket.getRemoteSocketAddress(),
      Optional.empty()
    );
  }

  private static void publish(RequestContext requestContext, Event event) {
    requestContext.environment().eventBus().publish(event);
  }

  /**
   * Returns the request as text: the request line, headers (with the Authorization header's value redacted),
   * and body.
   */
  static String describe(Request request) throws IOException {
    StringBuilder sb = new StringBuilder()
      .append(request.method()).append(' ').append(request.url().encodedPath());
    if (request.url().encodedQuery() != null) {
      sb.append('?').append(request.url().encodedQuery());
    }
    sb.append(" HTTP/1.1\n");
    appendHeaders(sb, request.headers(), name -> name.equalsIgnoreCase("Authorization"));

    RequestBody body = request.body();
    if (body != null) {
      Buffer buffer = new Buffer();
      body.writeTo(buffer);
      byte[] bytes = buffer.readByteArray();
      sb.append('\n').append(isTextual(request.header("Content-Type"))
        ? framedText(new String(bytes, UTF_8), 0, bytes.length)
        : hexDump(bytes, 0));
    }
    return sb.toString();
  }

  /**
   * Returns true if a body with the given content type is text (in UTF-8), so it's captured as text,
   * rather than as a hex dump: text, JSON, XML, and form data, unless another charset is specified.
   */
  static boolean isTextual(@Nullable String contentType) {
    MediaType mediaType = contentType == null ? null : MediaType.parse(contentType);
    if (mediaType == null) {
      return false; // unknown: hex dump
    }
    Charset charset = mediaType.charset(null);
    if (charset != null && !charset.equals(UTF_8)) {
      return false;
    }
    String type = mediaType.type().toLowerCase(Locale.ROOT);
    String subtype = mediaType.subtype().toLowerCase(Locale.ROOT);
    return type.equals("text")
      || subtype.equals("json") || subtype.endsWith("+json")
      || subtype.equals("xml") || subtype.endsWith("+xml")
      || subtype.equals("x-www-form-urlencoded");
  }

  /**
   * Returns the text of a body (or a chunk of one) between a header line, with its offset in the body and its
   * length in bytes, and a footer line, so where it starts and ends is clear (for example, if it starts or ends
   * with whitespace).
   */
  static String framedText(String text, long offset, long byteCount) {
    return String.format("+---- text: offset 0x%08x, %d bytes ----+%n", offset, byteCount)
      + text
      + String.format("%n+---- end of text ----+");
  }

  /**
   * Returns a hex dump of the bytes, in the same format as the Netty endpoints' traffic capture, except that
   * each row is labelled with its offset in the body: the bytes start at the given offset (for example,
   * a chunk of a response body after the first). Netty's dumps start at zero for each buffer.
   */
  static String hexDump(byte[] bytes, long startOffset) {
    if (bytes.length == 0) {
      return "";
    }
    String newline = System.lineSeparator(); // like Netty's
    StringBuilder sb = new StringBuilder()
      .append("         +-------------------------------------------------+").append(newline)
      .append("         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |").append(newline)
      .append("+--------+-------------------------------------------------+----------------+");
    for (int row = 0; row < bytes.length; row += 16) {
      sb.append(newline).append('|');
      appendHex(sb, startOffset + row, 8);
      sb.append('|');
      StringBuilder ascii = new StringBuilder();
      for (int i = row; i < row + 16; i++) {
        if (i < bytes.length) {
          int b = bytes[i] & 0xff;
          sb.append(' ');
          appendHex(sb, b, 2);
          ascii.append(b >= 0x20 && b < 0x7f ? (char) b : '.');
        } else {
          sb.append("   ");
          ascii.append(' ');
        }
      }
      sb.append(" |").append(ascii).append('|');
    }
    return sb.append(newline).append("+--------+-------------------------------------------------+----------------+").toString();
  }

  private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

  /**
   * Appends the value's lowest {@code digits} hex digits, with leading zeros: like String.format("%0<digits>x")
   * for values that fit, without its cost (hex dumps format every byte).
   */
  private static void appendHex(StringBuilder sb, long value, int digits) {
    for (int shift = (digits - 1) * 4; shift >= 0; shift -= 4) {
      sb.append(HEX_DIGITS[(int) (value >>> shift) & 0xf]);
    }
  }

  /**
   * Returns the response's status line and headers as text.
   */
  static String describe(Response response) {
    // Protocol.toString() is lowercase ("http/1.1"); the status line has it in uppercase.
    StringBuilder sb = new StringBuilder()
      .append(response.protocol().toString().toUpperCase(Locale.ROOT)).append(' ').append(response.code()).append(' ').append(response.message()).append('\n');
    appendHeaders(sb, response.headers(), name -> false);
    return sb.toString();
  }

  private static void appendHeaders(StringBuilder sb, Headers headers, Function<String, Boolean> redact) {
    for (int i = 0; i < headers.size(); i++) {
      String name = headers.name(i);
      sb.append(name).append(": ").append(redact.apply(name) ? "<redacted>" : headers.value(i)).append('\n');
    }
  }

  @Override
  public String toString() {
    return "TrafficCaptureInterceptor{servicesToCapture=" + servicesToCapture + "}";
  }

  /**
   * Decodes a body that arrives in chunks as UTF-8, without garbling a character split between two chunks:
   * the bytes of a character a chunk doesn't finish are held back and decoded with the next chunk
   * (by the JDK's {@link CharsetDecoder}, which handles input that arrives in pieces).
   * <p>
   * Not thread-safe (one per body, which is read on one thread at a time).
   */
  static final class Utf8ChunkDecoder {
    // Like new String(bytes, UTF_8), it replaces malformed input with U+FFFD.
    private final CharsetDecoder decoder = UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE);
    private ByteBuffer pending = ByteBuffer.allocate(0);
    private long decodedBytes;

    /**
     * Returns how many bytes have been decoded so far (not counting any held back): the offset in the body
     * of the next text it returns.
     */
    long decodedBytes() {
      return decodedBytes;
    }

    /**
     * Returns the text of the chunk (after any bytes held back from the previous one), except for a trailing
     * incomplete character, which it holds back.
     */
    String decode(byte[] chunk) {
      return decode(chunk, false);
    }

    /**
     * Returns the text of any bytes held back (at the end of the body, an incomplete character is malformed).
     */
    String flush() {
      return decode(new byte[0], true);
    }

    private String decode(byte[] chunk, boolean endOfInput) {
      ByteBuffer in = ByteBuffer.allocate(pending.remaining() + chunk.length).put(pending).put(chunk);
      in.flip();
      // Each byte decodes to at most one char (a 4-byte character is two), so this is big enough.
      CharBuffer out = CharBuffer.allocate(in.remaining());
      // Unless it's the end of the input, the decoder stops before a trailing incomplete character.
      decoder.decode(in, out, endOfInput);
      if (endOfInput) {
        decoder.flush(out);
      }
      decodedBytes += in.position();
      pending = in.slice(); // a trailing incomplete character, if any
      out.flip();
      return out.toString();
    }
  }

}
