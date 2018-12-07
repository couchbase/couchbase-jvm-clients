package com.couchbase.client.java.query;

public class QueryRow {

  private final byte[] encoded;

  public QueryRow(byte[] encoded) {
    this.encoded = encoded;
  }
}
