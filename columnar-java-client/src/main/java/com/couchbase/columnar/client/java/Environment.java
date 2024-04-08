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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.columnar.client.java.codec.Deserializer;

import static com.couchbase.client.core.util.Validators.notNull;
import static java.util.Objects.requireNonNull;

class Environment extends CoreEnvironment {
  private final Deserializer deserializer;

  private Environment(Builder builder) {
    super(builder);
    this.deserializer = requireNonNull(builder.deserializer);
  }

  @Override
  protected String defaultAgentTitle() {
    return "columnar-java";
  }

  /**
   * Returns the default serializer used to serialize and deserialize JSON values.
   */
  public Deserializer deserializer() {
    return deserializer;
  }

  static class Builder extends CoreEnvironment.Builder<Builder> {
    private Deserializer deserializer;

    Builder() {
      super();
    }

    public Builder deserializer(final Deserializer deserializer) {
      this.deserializer = notNull(deserializer, "deserializer");
      return this;
    }

    public Environment build() {
      return new Environment(this);
    }
  }
}
