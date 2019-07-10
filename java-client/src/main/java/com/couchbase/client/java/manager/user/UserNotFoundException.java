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

package com.couchbase.client.java.manager.user;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class UserNotFoundException extends CouchbaseException {
  private final AuthDomain domain;
  private final String username;

  public UserNotFoundException(AuthDomain domain, String username) {
    super("User [" + redactUser(username) + "] not found in " + domain + " domain.");
    this.domain = requireNonNull(domain);
    this.username = requireNonNull(username);
  }

  public static UserNotFoundException forUser(AuthDomain domain, String username) {
    return new UserNotFoundException(domain, username);
  }

  public AuthDomain domain() {
    return domain;
  }

  public String username() {
    return username;
  }
}
