package com.couchbase.client.java.codec;

public class BinaryContent {

  private final byte[] content;

  public static BinaryContent wrap(byte[] content) {
    return new BinaryContent(content);
  }

  private BinaryContent(byte[] content) {
    this.content = content;
  }

  public byte[] content() {
    return content;
  }
}
