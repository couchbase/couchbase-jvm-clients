/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * Returned from all kinds of KeyValue Get operation to fetch a document or a subset of it.
 *
 * @since 3.0.0
 */
public class GetResult {

  /**
   * The encoded content when loading the document.
   */
  protected final byte[] content;

  /**
   * The flags from the kv operation.
   */
  protected final int flags;

  /**
   * The CAS of the fetched document.
   */
  private final long cas;

  /**
   * The expiry if fetched and present.
   */
  private final Optional<Instant> expiry;

  /**
   * The default transcoder which should be used.
   */
  protected final Transcoder transcoder;

  /**
   * Creates a new {@link GetResult}.
   *
   * @param cas the cas from the doc.
   * @param expiry the expiry if fetched from the doc.
   */
  @Stability.Internal
  public GetResult(final byte[] content, final int flags, final long cas, final Optional<Instant> expiry, Transcoder transcoder) {
    this.cas = cas;
    this.content = content;
    this.flags = flags;
    this.expiry = expiry;
    this.transcoder = transcoder;
  }

  @Stability.Internal
  public GetResult(CoreGetResult core, Transcoder transcoder) {
    this(core.content(), core.flags(), core.cas(), Optional.ofNullable(core.expiry()), transcoder);
  }

  /**
   * Returns the CAS value of document at the time of loading.
   * <p>
   * The CAS value is an opaque identifier which is associated with a specific state of the document on the server. It
   * can be used during a subsequent mutation to make sure that the document has not been modified in the meantime.
   * <p>
   * If document on the server has been modified in the meantime the SDK will raise a {@link CasMismatchException}. In
   * this case the caller is expected to re-do the whole "fetch-modify-update" cycle again. Please refer to the
   * SDK documentation for more information on CAS mismatches and subsequent retries.
   */
  public long cas() {
    return cas;
  }

  /**
   * If the document has an expiry, returns length of time between
   * the start of the epoch and the point in time when the loaded
   * document expires.
   * <p>
   * In other words, the number of seconds in the returned duration
   * is equal to the epoch second when the document expires.
   * <p>
   * NOTE: This method always returns an empty Optional unless
   * the Get request was made using {@link GetOptions#withExpiry(boolean)}
   * set to true.
   */
  @Deprecated
  public Optional<Duration> expiry() {
    return expiry.map(instant -> Duration.ofSeconds(instant.getEpochSecond()));
  }

  /**
   * If the document has an expiry, returns the point in time when the loaded
   * document expires.
   * <p>
   * NOTE: This method always returns an empty Optional unless
   * the Get request was made using {@link GetOptions#withExpiry(boolean)}
   * set to true.
   */
  public Optional<Instant> expiryTime() {
    return expiry;
  }

  /**
   * Decodes the content of the document into a {@link JsonObject}.
   */
  public JsonObject contentAsObject() {
    return contentAs(JsonObject.class);
  }

  /**
   * Decodes the content of the document into a {@link JsonArray}.
   */
  public JsonArray contentAsArray() {
    return contentAs(JsonArray.class);
  }

  /**
   * Decodes the content of the document into an instance of the target class.
   *
   * @param target the target class to decode the encoded content into.
   */
  public <T> T contentAs(final Class<T> target) {
    return transcoder.decode(target, content, flags);
  }

  /**
   * Decodes the content of the document into an instance of the target type.
   * Example usage:
   * <pre>
   * List&lt;String&gt; strings = result.contentAs(new TypeRef&lt;List&lt;String&gt;&gt;(){});
   * </pre>
   *
   * @param target the type to decode the encoded content into.
   */
  public <T> T contentAs(final TypeRef<T> target) {
    return transcoder.decode(target, content, flags);
  }

  /**
   * Returns the raw bytes of the document content.
   *
   * @return the document content as a byte array
   */
  @Stability.Uncommitted
  public byte[] contentAsBytes() {
    return content;
  }

  @Override
  public String toString() {
    return "GetResult{" +
      "content=" + redactUser(convertContentToString(content, flags)) +
      ", flags=0x" + Integer.toHexString(flags) +
      ", cas=0x" + Long.toHexString(cas) +
      ", expiry=" + expiry +
      '}';
  }

  /**
   * Converts the content to a string representation if possible (for toString).
   */

  static String convertContentToString(final byte[] content, final int flags) {
    if (content.length == 0) {
      return "";
    }

    boolean printable = CodecFlags.hasCommonFormat(flags, CodecFlags.JSON_COMMON_FLAGS)
      || CodecFlags.hasCommonFormat(flags, CodecFlags.STRING_COMMON_FLAGS)
      || (flags == 0 && content[0] == '{') ;

    if (printable) {
      return new String(content, StandardCharsets.UTF_8);
    } else {
      ByteBuf buf = Unpooled.wrappedBuffer(content);
      String result = ByteBufUtil.prettyHexDump(buf);
      buf.release();
      return "\n" + result + "\n";
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetResult getResult = (GetResult) o;
    return flags == getResult.flags &&
      cas == getResult.cas &&
      Arrays.equals(content, getResult.content) &&
      Objects.equals(expiry, getResult.expiry) &&
      Objects.equals(transcoder, getResult.transcoder);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(flags, cas, expiry, transcoder);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }
}
