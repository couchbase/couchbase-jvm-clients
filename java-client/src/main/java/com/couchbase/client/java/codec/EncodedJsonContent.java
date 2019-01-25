package com.couchbase.client.java.codec;

public class EncodedJsonContent {

  private byte[] content;

  public static EncodedJsonContent wrap(byte[] content) {
    return new EncodedJsonContent(content);
  }

  private EncodedJsonContent(byte[] content) {
    this.content = content;
  }

  public byte[] content() {
    return content;
  }
}
