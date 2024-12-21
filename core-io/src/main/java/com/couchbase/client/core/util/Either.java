/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@Stability.Internal
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class Either<L, R> {
  private final Optional<L> left;
  private final Optional<R> right;

  /**
   * @throws IllegalArgumentException if left and right are both null or both non-null
   */
  public static <L, R> Either<L, R> of(
    @Nullable L left,
    @Nullable R right
  ) {
    return new Either<>(left, right);
  }

  public static <L, R> Either<L, R> ofLeft(L left) {
    requireNonNull(left, "`Either` cannot hold null values");
    return new Either<>(left, null);
  }

  public static <L, R> Either<L, R> ofRight(R right) {
    requireNonNull(right, "`Either` cannot hold null values");
    return new Either<>(null, right);
  }

  private Either(
    @Nullable L left,
    @Nullable R right
  ) {
    if (left == null && right == null) {
      throw new IllegalArgumentException("Exactly one of left/right must be non-null, but both were null.");
    }
    if (left != null && right != null) {
      throw new IllegalArgumentException("Exactly one of left/right must be non-null, but both were non-null.");
    }

    this.left = Optional.ofNullable(left);
    this.right = Optional.ofNullable(right);
  }

  public Optional<L> left() {
    return left;
  }

  public Optional<R> right() {
    return right;
  }

  public void ifPresent(
    Consumer<L> leftConsumer,
    Consumer<R> rightConsumer
  ) {
    left().ifPresent(leftConsumer);
    right().ifPresent(rightConsumer);
  }

  @Override
  public String toString() {
    return "Either{" +
      "left=" + left.orElse(null) +
      ", right=" + right.orElse(null) +
      '}';
  }
}
