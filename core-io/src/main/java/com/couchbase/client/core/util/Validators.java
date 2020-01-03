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

package com.couchbase.client.core.util;

import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Common validators used throughout the client.
 *
 * @since 2.0.0
 */
public class Validators {

  private Validators() {}

  /**
   * Check if the given input is not null.
   *
   * <p>If it is null, a {@link InvalidArgumentException} is raised with a proper message.</p>
   *
   * @param input the input to check.
   * @param identifier the identifier that is part of the exception message.
   */
  public static <T> T notNull(final T input, final String identifier) {
    if (input == null) {
      throw InvalidArgumentException.fromMessage(identifier + " cannot be null");
    }
    return input;
  }

  public static <T> T notNull(final T input, final String identifier, final Supplier<ErrorContext> errorContext) {
    try {
      return notNull(input, identifier);
    } catch (Exception cause) {
      throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
    }
  }

  /**
   * Check if the given string is not null or empty.
   *
   * <p>If it is null or empty, a {@link InvalidArgumentException} is raised with a
   * proper message.</p>
   *
   * @param input the string to check.
   * @param identifier the identifier that is part of the exception message.
   */
  public static String notNullOrEmpty(final String input, final String identifier) {
    if (input == null || input.isEmpty()) {
      throw InvalidArgumentException.fromMessage(identifier + " cannot be null or empty");
    }
    return input;
  }

  public static String notNullOrEmpty(final String input, final String identifier,
                                      final Supplier<ErrorContext> errorContext) {
    try {
      return notNullOrEmpty(input, identifier);
    } catch (Exception cause) {
      throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
    }
  }

  public static <T> List<T> notNullOrEmpty(final List<T> input, final String identifier) {
    if (input == null || input.isEmpty()) {
      throw InvalidArgumentException.fromMessage(identifier + " cannot be null or empty");
    }
    return input;
  }

  public static <T> List<T> notNullOrEmpty(final List<T> input, final String identifier,
                                           final Supplier<ErrorContext> errorContext) {
    try {
      return notNullOrEmpty(input, identifier);
    } catch (Exception cause) {
      throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
    }
  }

  public static <T> Set<T> notNullOrEmpty(final Set<T> input, final String identifier) {
    if (input == null || input.isEmpty()) {
      throw InvalidArgumentException.fromMessage(identifier + " cannot be null or empty");
    }
    return input;
  }

  public static <T> Set<T> notNullOrEmpty(final Set<T> input, final String identifier,
                                          final Supplier<ErrorContext> errorContext) {
    try {
      return notNullOrEmpty(input, identifier);
    } catch (Exception cause) {
      throw new InvalidArgumentException("Argument validation failed", cause, errorContext.get());
    }
  }

}
