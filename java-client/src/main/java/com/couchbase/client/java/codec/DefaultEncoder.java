package com.couchbase.client.java.codec;

import java.util.function.Function;

public class DefaultEncoder<T> implements Function<T, byte[]> {

  public static DefaultEncoder<Object> ENCODER = new DefaultEncoder<>();

  private DefaultEncoder() {}

  @Override
  public byte[] apply(T t) {
    return new byte[0];
  }

}
