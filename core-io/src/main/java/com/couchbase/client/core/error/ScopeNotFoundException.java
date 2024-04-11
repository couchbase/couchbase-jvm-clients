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
import com.couchbase.client.core.error.context.ErrorContext;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class ScopeNotFoundException extends CouchbaseException {
  private final String scopeName;

  @Stability.Internal
  public ScopeNotFoundException(String scopeName, @Nullable ErrorContext context) {
    super("Scope" + formatName(scopeName) + " not found.", context);
    this.scopeName = requireNonNull(scopeName);
  }

  private static String formatName(String name) {
    return isNullOrEmpty(name) ? "" : " [" + redactMeta(name) + "]";
  }

  public static ScopeNotFoundException forScope(String scopeName) {
    return new ScopeNotFoundException(scopeName, null);
  }

  public String scopeName() {
    return scopeName;
  }
}
