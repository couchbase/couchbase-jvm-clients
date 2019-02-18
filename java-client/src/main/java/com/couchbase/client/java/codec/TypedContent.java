package com.couchbase.client.java.codec;

abstract class TypedContent {

  private final byte[] content;

  TypedContent(byte[] content) {
    this.content = content;
  }

  public byte[] content() {
    return content;
  }

  public abstract int flags();

}
