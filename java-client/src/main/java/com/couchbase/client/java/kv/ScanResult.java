/*
 * Copyright (c) 2022 Couchbase, Inc.
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
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.java.kv.GetResult.convertContentToString;

/**
 * Returned for each found item in a KV Range Scan operation.
 */
@Stability.Volatile
public class ScanResult {

  /**
   * The document ID.
   */
  private final String id;

  /**
   * The encoded content when loading the document.
   */
  private final byte[] content;

  /**
   * The flags from the kv operation.
   */
  private final int flags;

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
  private final Transcoder transcoder;

  /**
   * True if this result contains only a document ID.
   * <p>
   * If true, any attempt to get the document content or metadata will fail.
   */
  private final boolean idOnly;

  /**
   * Creates a new {@link GetResult}.
   *
   * @param cas the cas from the doc.
   * @param expiry the expiry if fetched from the doc.
  */
  @Stability.Internal
  public ScanResult(final boolean idOnly, final String id, final byte[] content, final int flags, final long cas,
                    final Optional<Instant> expiry, final Transcoder transcoder) {
    this.id = id;
    this.idOnly = idOnly;
    this.cas = cas;
    this.content = content;
    this.flags = flags;
    this.expiry = expiry;
    this.transcoder = transcoder;
  }

  /**
   * Returns the ID of the document.
   *
   * @return the ID of the document.
   */
  public String id() {
    return id;
  }

  /**
   * Returns true if this result came from a scan where {@link ScanOptions#idsOnly(boolean)}
   * was set to true.
   * <p>
   * If true, attempting to access the document content or metadata will throw
   * {@link UnsupportedOperationException}.
   */
  public boolean idOnly() {
    return idOnly;
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
   *
   * @throws UnsupportedOperationException if this result came from a scan
   * where {@link ScanOptions#idsOnly(boolean)} was set to true. See {@link #idOnly()}.
   */
  public long cas() {
    requireBody();
    return cas;
  }

  /**
   * If the document has an expiry, returns the point in time when the loaded
   * document expires.
   * <p>
   * @throws UnsupportedOperationException if this result came from a scan
   * where {@link ScanOptions#idsOnly(boolean)} was set to true. See {@link #idOnly()}.
   */
  public Optional<Instant> expiryTime() {
    requireBody();
    return expiry;
  }

  /**
   * Decodes the content of the document into a {@link JsonObject}.
   *
   * @throws UnsupportedOperationException if this result came from a scan
   * where {@link ScanOptions#idsOnly(boolean)} was set to true. See {@link #idOnly()}.
   */
  public JsonObject contentAsObject() {
    return contentAs(JsonObject.class);
  }

  /**
   * Decodes the content of the document into a {@link JsonArray}.
   *
   * @throws UnsupportedOperationException if this result came from a scan
   * where {@link ScanOptions#idsOnly(boolean)} was set to true. See {@link #idOnly()}.
   */
  public JsonArray contentAsArray() {
    return contentAs(JsonArray.class);
  }

  /**
   * Decodes the content of the document into an instance of the target class.
   *
   * @param target the target class to decode the encoded content into.
   * @throws UnsupportedOperationException if this result came from a scan
   * where {@link ScanOptions#idsOnly(boolean)} was set to true. See {@link #idOnly()}.
   */
  public <T> T contentAs(final Class<T> target) {
    requireBody();
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
   * @throws UnsupportedOperationException if this result came from a scan
   * where {@link ScanOptions#idsOnly(boolean)} was set to true. See {@link #idOnly()}.
   */
  public <T> T contentAs(final TypeRef<T> target) {
    requireBody();
    return transcoder.decode(target, content, flags);
  }

  /**
   * Returns the raw bytes of the document content.
   *
   * @return the document content as a byte array
   * @throws UnsupportedOperationException if this result came from a scan
   * where {@link ScanOptions#idsOnly(boolean)} was set to true. See {@link #idOnly()}.
   */
  public byte[] contentAsBytes() {
    requireBody();
    return content;
  }

  private void requireBody() {
    if (idOnly) {
      throw new UnsupportedOperationException("This result came from a scan configured to return only document IDs.");
    }
  }

  @Override
  public String toString() {
    return "ScanResult{" +
      "id=" + redactMeta(id) +
      ", content=" + redactUser(convertContentToString(content, flags)) +
      ", flags=0x" + Integer.toHexString(flags) +
      ", cas=0x" + Long.toHexString(cas) +
      ", expiry=" + expiry +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ScanResult that = (ScanResult) o;
    return flags == that.flags && cas == that.cas && Objects.equals(id, that.id)
      && Arrays.equals(content, that.content) && Objects.equals(expiry, that.expiry)
      && Objects.equals(transcoder, that.transcoder);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, flags, cas, expiry, transcoder);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }

}
