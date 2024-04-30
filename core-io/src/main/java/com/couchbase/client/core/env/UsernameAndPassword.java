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

package com.couchbase.client.core.env;

import static java.util.Objects.requireNonNull;

public class UsernameAndPassword {
  private final String username;
  private final String password;

  public UsernameAndPassword(String username, String password) {
    this.username = requireNonNull(username);
    this.password = requireNonNull(password);
  }

  public String username() {
    return username;
  }

  public String password() {
    return password;
  }

  @Override
  public String toString() {
    // Whatever else you change, don't include the password.
    // It must never end up in the logs or on the console.
    return "UsernameAndPassword{" +
      "username='" + username + '\'' +
      '}';
  }
}
