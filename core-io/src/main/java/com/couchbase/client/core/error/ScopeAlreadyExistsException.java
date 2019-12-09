/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class ScopeAlreadyExistsException extends CouchbaseException {
  private final String scopeName;

  public ScopeAlreadyExistsException(String scopeName) {
    super("Scope [" + redactMeta(scopeName) + "] already exists.");
    this.scopeName = requireNonNull(scopeName);
  }

  public static ScopeAlreadyExistsException forScope(String scopeName) {
    return new ScopeAlreadyExistsException(scopeName);
  }

  public String scopeName() {
    return scopeName;
  }
}
