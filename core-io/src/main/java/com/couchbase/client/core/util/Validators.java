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

import java.util.List;

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
   * <p>If it is null, a {@link IllegalArgumentException} is raised with a proper message.</p>
   *
   * @param input the input to check.
   * @param identifier the identifier that is part of the exception message.
   */
  public static void notNull(final Object input, final String identifier) {
    if (input == null) {
      throw new IllegalArgumentException(identifier + " cannot be null");
    }
  }

  /**
   * Check if the given string is not null or empty.
   *
   * <p>If it is null or empty, a {@link IllegalArgumentException} is raised with a
   * proper message.</p>
   *
   * @param input the string to check.
   * @param identifier the identifier that is part of the exception message.
   */
  public static void notNullOrEmpty(final String input, final String identifier) {
    if (input == null || input.isEmpty()) {
      throw new IllegalArgumentException(identifier + " cannot be null or empty");
    }
  }

  public static void notNullOrEmpty(final List<?> input, final String identifier) {
    if (input == null || input.isEmpty()) {
      throw new IllegalArgumentException(identifier + " cannot be null or empty");
    }
  }

}
