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

public class EventingFunctionBucketBinding {

  private String alias;
  private EventingFunctionKeyspace name;
  private EventingFunctionBucketAccess access;

  public EventingFunctionBucketBinding(String alias, EventingFunctionKeyspace name, EventingFunctionBucketAccess access) {
    this.alias = alias;
    this.name = name;
    this.access = access;
  }

  public String alias() {
    return alias;
  }

  public EventingFunctionBucketBinding alias(String alias) {
    this.alias = alias;
    return this;
  }

  public EventingFunctionKeyspace name() {
    return name;
  }

  public EventingFunctionBucketBinding name(EventingFunctionKeyspace name) {
    this.name = name;
    return this;
  }

  public EventingFunctionBucketAccess access() {
    return access;
  }

  public EventingFunctionBucketBinding access(EventingFunctionBucketAccess access) {
    this.access = access;
    return this;
  }

  @Override
  public String toString() {
    return "EventingFunctionBucketBinding{" +
      "alias='" + redactMeta(alias) + '\'' +
      ", name=" + redactUser(name) +
      ", access=" + access +
      '}';
  }
}
