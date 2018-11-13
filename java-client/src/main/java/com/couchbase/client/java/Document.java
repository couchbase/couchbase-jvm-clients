package com.couchbase.client.java;

public class Document<T> {

  public Document(String id, T content, long cas) {
    this.id = id;
    this.content = content;
    this.cas = cas;
  }

  private final String id;

  private final T content;

  private final long cas;


  public String id() {
    return id;
  }

  public T content() {
    return content;
  }

  public long cas() {
    return cas;
  }

}
