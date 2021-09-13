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

package com.couchbase.client.java.manager.eventing;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Represents a constant binding of an eventing function.
 */
public class EventingFunctionConstantBinding {

  private final String alias;
  private final String literal;

  /**
   * Creates a new {@link EventingFunctionConstantBinding}.
   *
   * @param alias the alias for the constant binding.
   * @param literal the literal of the constant binding (the constant itself).
   * @return a new instance of the {@link EventingFunctionConstantBinding}.
   */
  public static EventingFunctionConstantBinding create(final String alias, final String literal) {
    return new EventingFunctionConstantBinding(alias, literal);
  }

  private EventingFunctionConstantBinding(final String alias, final String literal) {
    this.alias = notNullOrEmpty(alias, "Alias");
    this.literal = notNullOrEmpty(literal, "Literal");
  }

  /**
   * The alias for the eventing function.
   */
  public String alias() {
    return alias;
  }

  /**
   * The literal for the eventing function.
   */
  public String literal() {
    return literal;
  }

  @Override
  public String toString() {
    return "EventingFunctionConstantBinding{" +
      "alias='" + redactMeta(alias) + '\'' +
      ", literal='" + redactUser(literal) + '\'' +
      '}';
  }

}
