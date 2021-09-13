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
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Represents a bucket binding of an eventing function.
 */
public class EventingFunctionBucketBinding {

  private final String alias;
  private final EventingFunctionKeyspace keyspace;
  private final EventingFunctionBucketAccess access;

  /**
   * Creates a read-only bucket binding.
   *
   * @param alias the alias for the bucket binding.
   * @param keyspace the keyspace to which the binding points to.
   * @return a created {@link EventingFunctionBucketBinding}.
   */
  public static EventingFunctionBucketBinding createReadOnly(String alias, EventingFunctionKeyspace keyspace) {
    return new EventingFunctionBucketBinding(alias, keyspace, EventingFunctionBucketAccess.READ_ONLY);
  }

  /**
   * Creates a read-write bucket binding.
   *
   * @param alias the alias for the bucket binding.
   * @param keyspace the keyspace to which the binding points to.
   * @return a created {@link EventingFunctionBucketBinding}.
   */
  public static EventingFunctionBucketBinding createReadWrite(String alias, EventingFunctionKeyspace keyspace) {
    return new EventingFunctionBucketBinding(alias, keyspace, EventingFunctionBucketAccess.READ_WRITE);
  }

  private EventingFunctionBucketBinding(String alias, EventingFunctionKeyspace keyspace,
                                        EventingFunctionBucketAccess access) {
    this.alias = notNullOrEmpty(alias, "Alias");
    this.keyspace = notNull(keyspace, "Keyspace");
    this.access = notNull(access, "Access");
  }

  /**
   * Returns the alias for the bucket binding.
   */
  public String alias() {
    return alias;
  }

  /**
   * Returns the keyspace triple this bucket is accessing.
   */
  public EventingFunctionKeyspace keyspace() {
    return keyspace;
  }

  /**
   * Returns the bucket access policy for the bucket binding.
   */
  public EventingFunctionBucketAccess access() {
    return access;
  }

  @Override
  public String toString() {
    return "EventingFunctionBucketBinding{" +
      "alias='" + redactMeta(alias) + '\'' +
      ", keyspace=" + redactUser(keyspace) +
      ", access=" + access +
      '}';
  }
}
