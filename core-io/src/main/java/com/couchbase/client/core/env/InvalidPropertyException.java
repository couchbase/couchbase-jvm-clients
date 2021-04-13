/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.error.InvalidArgumentException;

/**
 * Thrown when an environment config property cannot be applied.
 */
public class InvalidPropertyException extends InvalidArgumentException {
  private final String propertyName;
  private final String propertyValue;

  private InvalidPropertyException(String propertyName, String propertyValue, Throwable cause) {
    super("Failed to apply environment config property '" + propertyName + "'" +
        " with value '" + propertyValue + "'. " + cause.getMessage(), cause, null);
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
  }

  public static InvalidPropertyException forProperty(String propertyName, String propertyValue, Throwable cause) {
    return new InvalidPropertyException(propertyName, propertyValue, cause);
  }

  public String propertyName() {
    return propertyName;
  }

  public String propertyValue() {
    return propertyValue;
  }
}
