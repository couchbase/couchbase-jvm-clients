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

public class EventingFunctionConstantBinding {

  private String alias;
  private String literal;

  public EventingFunctionConstantBinding(final String alias, final String literal) {
    this.alias = alias;
    this.literal = literal;
  }

  public String alias() {
    return alias;
  }

  public String literal() {
    return literal;
  }

  public EventingFunctionConstantBinding alias(String alias) {
    this.alias = alias;
    return this;
  }

  public EventingFunctionConstantBinding literal(String literal) {
    this.literal = literal;
    return this;
  }

  @Override
  public String toString() {
    return "EventingFunctionConstantBinding{" +
      "alias='" + redactMeta(alias) + '\'' +
      ", literal='" + redactUser(literal) + '\'' +
      '}';
  }
}
