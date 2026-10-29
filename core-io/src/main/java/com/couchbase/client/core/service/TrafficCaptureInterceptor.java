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
import com.couchbase.client.core.deps.okhttp3.Connection;
import com.couchbase.client.core.deps.okhttp3.Headers;
import com.couchbase.client.core.deps.okhttp3.Interceptor;
import com.couchbase.client.core.deps.okhttp3.MediaType;
import com.couchbase.client.core.deps.okhttp3.Request;
import com.couchbase.client.core.deps.okhttp3.RequestBody;
import com.couchbase.client.core.deps.okhttp3.Response;
import com.couchbase.client.core.deps.okhttp3.ResponseBody;
import com.couchbase.client.core.deps.okio.Buffer;
import com.couchbase.client.core.deps.okio.ForwardingSource;
import com.couchbase.client.core.deps.okio.Okio;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.TrafficCaptureFormat;
import com.couchbase.client.core.io.TrafficCaptureMasking;
import com.couchbase.client.core.msg.RequestContext;
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
import java.util.function.BiFunction;

import static com.couchbase.client.core.io.TrafficCaptureMasking.MASKED;
import static com.couchbase.client.core.io.TrafficCaptureMasking.maskAuthorization;
import static com.couchbase.client.core.io.TrafficCaptureMasking.maskSecretFields;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Captures the HTTP traffic of the services in {@link IoConfig#servicesToCapture()}: publishes a {@link WriteTrafficCapturedEvent} for each request as it's sent
 * (request line, headers, and body), and {@link ReadTrafficCapturedEvent}s for its response as it's received
 * (status line and headers, then each chunk of the body as the caller reads it).
 * <p>
 * Request and status lines and headers are captured as text. A body is captured as text if its content type is
 * textual (see {@link #isTextual}); a character split between two chunks is captured whole, with the later chunk.
 * Otherwise, it's captured as a hex dump (see {@link TrafficCaptureFormat}).
 * <p>
 * This captures the HTTP messages as OkHttp sends and receives them, mostly as text: OkHttp's own framing (like chunked encoding) isn't included.
 * (Capturing the bytes inside a TLS connection would mean wrapping OkHttp's SSLSocket, which OkHttp depends on
 * the exact type of.)
 * <p>
 * Credentials in requests are masked (see {@link TrafficCaptureMasking}): the credential
 * in the Authorization header (keeping the auth scheme), the Content-Length header's value, and the values of known
 * secret fields in form-encoded and JSON bodies.
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
              captured = TrafficCaptureFormat.hexDump(bytes, bodyOffset);
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
    appendHeaders(sb, request.headers(), TrafficCaptureInterceptor::maskRequestHeader);

    RequestBody body = request.body();
    if (body != null) {
      Buffer buffer = new Buffer();
      body.writeTo(buffer);
      byte[] bytes = buffer.readByteArray();
      sb.append('\n').append(isTextual(request.header("Content-Type"))
        ? maskedText(bytes)
        : TrafficCaptureFormat.hexDump(withSecretFieldsMasked(bytes)));
    }
    return sb.toString();
  }

  /**
   * Returns the value of a request header as captured: masked if it's a credential, or if it's Content-Length
   * (when a body field is masked, the original length would reveal the secret's length).
   */
  private static String maskRequestHeader(String name, String value) {
    if (name.equalsIgnoreCase("Authorization")) {
      return maskAuthorization(value);
    }
    if (name.equalsIgnoreCase("Content-Length")) {
      return MASKED;
    }
    return value;
  }

  /**
   * Returns a textual request body as framed text, with the values of secret fields masked. If any were masked,
   * the frame gives the length of the masked text, since the original length would reveal the secrets' lengths.
   */
  private static String maskedText(byte[] bytes) {
    String text = new String(bytes, UTF_8);
    String masked = maskSecretFields(text);
    return masked == text
      ? framedText(text, 0, bytes.length)
      : framedText(masked, 0, masked.getBytes(UTF_8).length);
  }

  /**
   * Returns a request body (that isn't text) with the values of secret fields masked.
   */
  private static byte[] withSecretFieldsMasked(byte[] bytes) {
    // ISO-8859-1 maps each byte to exactly one char, so the masked string converts back
    // to the original bytes everywhere except where something was masked.
    String original = new String(bytes, ISO_8859_1);
    String masked = maskSecretFields(original);
    return masked == original ? bytes : masked.getBytes(ISO_8859_1);
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
   * Returns the response's status line and headers as text.
   */
  static String describe(Response response) {
    // Protocol.toString() is lowercase ("http/1.1"); the status line has it in uppercase.
    StringBuilder sb = new StringBuilder()
      .append(response.protocol().toString().toUpperCase(Locale.ROOT)).append(' ').append(response.code()).append(' ').append(response.message()).append('\n');
    appendHeaders(sb, response.headers(), (name, value) -> value);
    return sb.toString();
  }

  /**
   * @param capturedValue given a header's name and value, returns the value to capture.
   */
  private static void appendHeaders(StringBuilder sb, Headers headers, BiFunction<String, String, String> capturedValue) {
    for (int i = 0; i < headers.size(); i++) {
      String name = headers.name(i);
      sb.append(name).append(": ").append(capturedValue.apply(name, headers.value(i))).append('\n');
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
