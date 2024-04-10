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
 * Simple functional Either.
 */
public class Either<T, U> {
  private final @Nullable T left;
  private final @Nullable U right;

  Either(@Nullable T left, @Nullable U right) {
    this.left = left;
    this.right = right;
    if (left == null && right == null) {
      throw new IllegalArgumentException("Both left and right can't be missing");
    }
    if (left != null && right != null) {
      throw new IllegalArgumentException("Both left and right can't be present");
    }
  }

  public boolean isLeft() {
    return left != null;
  }

  public boolean isRight() {
    return right != null;
  }

  public T left() {
    return left;
  }

  public U right() {
    return right;
  }

  public static <T, U> Either<T, U> left(T left) {
    return new Either<T, U>(left, null);
  }

  /**
   * Assume Either<RuntimeException, U> if it hasn't been made explicit.
   */
  public static <U> Either<RuntimeException, U> right(U t) {
    return new Either<RuntimeException, U>(null, t);
  }

  @Override
  public String toString() {
    return "Either{" +
            "left=" + left +
            ", right=" + right +
            '}';
  }
}
