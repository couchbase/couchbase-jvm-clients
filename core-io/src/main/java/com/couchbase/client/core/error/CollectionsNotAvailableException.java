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

package com.couchbase.client.core.error;

/**
 * This exception is raised when collections are not available on the connected cluster, likely
 * because a version is used that does not support them.
 *
 * <p>Make sure to distinguish this exception from {@link CollectionDoesNotExistException} which
 * is raised if collections are available, but the specific request collection does not exist.</p>
 *
 * @since 2.0.0
 */
public class CollectionsNotAvailableException extends CouchbaseException {

  public CollectionsNotAvailableException() {
  }

  public CollectionsNotAvailableException(String message) {
    super(message);
  }

  public CollectionsNotAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

  public CollectionsNotAvailableException(Throwable cause) {
    super(cause);
  }

  public CollectionsNotAvailableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
