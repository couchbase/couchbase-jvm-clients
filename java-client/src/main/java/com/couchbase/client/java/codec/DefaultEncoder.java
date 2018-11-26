/*
 * Copyright (c) 2018 Couchbase, Inc.
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
