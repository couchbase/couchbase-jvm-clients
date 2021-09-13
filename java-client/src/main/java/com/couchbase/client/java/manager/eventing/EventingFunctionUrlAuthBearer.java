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

/**
 * Uses Bearer authentication for the URL binding.
 * <p>
 * Created through {@link EventingFunctionUrlAuth#bearerAuth(String)}.
 */
public class EventingFunctionUrlAuthBearer extends EventingFunctionUrlAuth {

  /**
   * The bearer key to use.
   */
  private final String key;

  /**
   * Creates a new {@link EventingFunctionUrlAuthBearer} instance.
   * <p>
   * Created through {@link EventingFunctionUrlAuth#bearerAuth(String)}.
   *
   * @param key the key.
   */
  EventingFunctionUrlAuthBearer(String key) {
    this.key = key;
  }

  /**
   * The key used for bearer auth - not set if returned from the server for security reasons.
   *
   * @return the key if set.
   */
  public String key() {
    return key;
  }

  @Override
  public String toString() {
    return "EventingFunctionUrlAuthBearer{" +
      "key='*****'" +
      '}';
  }
}
