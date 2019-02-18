package com.couchbase.client.java.codec;

abstract class TypedContent<T> {

  private final T content;

  TypedContent(T content) {
    this.content = content;
  }

  public T content() {
    return content;
  }

  public abstract byte[] encoded();

  public abstract int flags();

}
