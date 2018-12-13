package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import io.netty.util.CharsetUtil;

/**
 * The {@link EncodedFragment} represents a fragment returned from a subdocument lookup.
 *
 * <p>While the surface area is pretty small, we consider this advanced API and therefore it
 * is not marked as commited at this point.</p>
 *
 * @since 3.0.0
 */
@Stability.Uncommitted
public class EncodedFragment {

  private final String path;
  private final byte[] content;

  public EncodedFragment(String path, byte[] content) {
    this.path = path;
    this.content = content;
  }

  public String path() {
    return path;
  }

  public byte[] content() {
    return content;
  }

  @Override
  public String toString() {
    return "EncodedFragment{" +
      "path=" + path +
      ", content=" + new String(content, CharsetUtil.UTF_8) +
      '}';
  }
}
