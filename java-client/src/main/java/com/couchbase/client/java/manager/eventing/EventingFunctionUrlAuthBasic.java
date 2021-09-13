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

/**
 * Uses HTTP Basic authentication for the URL binding.
 * <p>
 * Created through {@link EventingFunctionUrlAuth#basicAuth(String, String)}.
 */
public class EventingFunctionUrlAuthBasic extends EventingFunctionUrlAuth {

  /**
   * The username to use.
   */
  private final String username;

  /**
   * The password to use.
   */
  private final String password;

  /**
   * Creates a new {@link EventingFunctionUrlAuthBasic} instance.
   * <p>
   * Created through {@link EventingFunctionUrlAuth#basicAuth(String, String)}.
   *
   * @param username the username.
   * @param password the password.
   */
  EventingFunctionUrlAuthBasic(final String username, final String password) {
    this.username = username;
    this.password = password;
  }

  /**
   * The username that is used for basic auth.
   */
  public String username() {
    return username;
  }

  /**
   * The password that is used for basic auth - not set if returned from the server for security reasons.
   */
  public String password() {
    return password;
  }

  @Override
  public String toString() {
    return "EventingFunctionUrlAuthBasic{" +
      "username='" + redactMeta(username) + '\'' +
      ", password='*****'" +
      '}';
  }
}
