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
 * This is the parent class for all request cancellations and signals that the request
 * in question has been canceled for whatever reason.
 *
 * <p>This class may be subclassed to indicate why it has been cancelled to further aid
 * debugging.</p>
 *
 * @since 2.0.0
 */
public class RequestCanceledException extends CouchbaseException {

  public RequestCanceledException() {
  }

  public RequestCanceledException(String message) {
    super(message);
  }

  public RequestCanceledException(String message, Throwable cause) {
    super(message, cause);
  }

  public RequestCanceledException(Throwable cause) {
    super(cause);
  }

  public RequestCanceledException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
