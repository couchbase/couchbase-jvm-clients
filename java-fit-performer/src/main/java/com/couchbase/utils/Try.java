/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.utils;

import javax.annotation.Nullable;

/**
 * Analogous to Scala's Try and Kotlin's Result.  Essentially a right-biased Either<RuntimeException, T>.
 */
public class Try<T> {
  private final Either<RuntimeException, T> internal;

  public Try(T value) {
    this.internal = Either.right(value);
  }

  public Try(RuntimeException error) {
    this.internal = Either.left(error);
  }

  public boolean isSuccess() {
    return internal.isRight();
  }

  public boolean isFailure() {
    return internal.isLeft();
  }

  public RuntimeException exception() {
    return internal.left();
  }

  public T value() {
    return internal.right();
  }

  @Override
  public String toString() {
    return "Try{" +
            "internal=" + internal +
            '}';
  }
}
