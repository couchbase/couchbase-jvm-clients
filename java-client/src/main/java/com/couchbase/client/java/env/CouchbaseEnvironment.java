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

package com.couchbase.client.java.env;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PropertyLoader;

public class CouchbaseEnvironment extends CoreEnvironment {

  private CouchbaseEnvironment(Builder builder) {
    super(builder);
  }

  public static CouchbaseEnvironment create() {
    return builder().build();
  }

  public static CouchbaseEnvironment.Builder builder() {
    return new Builder();
  }

  public static class Builder extends CoreEnvironment.Builder<Builder> {

    public Builder load(final CouchbasePropertyLoader loader) {
      loader.load(this);
      return this;
    }

    public CouchbaseEnvironment build() {
      return new CouchbaseEnvironment(this);
    }
  }
}
