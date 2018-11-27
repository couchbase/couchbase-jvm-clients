package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

/**
 * The {@link EncodedDocument} has everything which is important for a document on the wire
 * and is needed for properly decoding it after reading it.
 *
 * <p>While the surface area is pretty small, we consider this advanced API and therefore it
 * is not marked as commited at this point.</p>
 *
 * @since 3.0.0
 */
@Stability.Uncommitted
public class EncodedDocument {

  private final int flags;
  private final byte[] content;

  public EncodedDocument(int flags, byte[] content) {
    this.flags = flags;
    this.content = content;
  }

  public int flags() {
    return flags;
  }

  public byte[] content() {
    return content;
  }

}
