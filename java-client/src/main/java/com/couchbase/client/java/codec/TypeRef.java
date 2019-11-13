/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.codec;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Conveys generic type information at run time.
 * <p>
 * Create an anonymous subclass parameterized with the type you want to represent.
 * For example:
 * <pre>
 * TypeRef&lt;List&lt;String>> listOfStrings = new TypeRef&lt;List&lt;String>>(){};
 * </pre>
 * Immutable.
 *
 * @param <T> The type to represent
 * @implNote Uses the technique described in Neal Gafter's article on
 * <a href="http://gafter.blogspot.com/2006/12/super-type-tokens.html">
 * Super Type Tokens</a>.
 */
public abstract class TypeRef<T> {
  private final Type type;

  protected TypeRef() {
    final Type superclass = getClass().getGenericSuperclass();
    if (superclass instanceof Class) {
      throw new RuntimeException("Missing type parameter.");
    }
    this.type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  public Type type() {
    return type;
  }

  public String toString() {
    return type.getTypeName();
  }
}
