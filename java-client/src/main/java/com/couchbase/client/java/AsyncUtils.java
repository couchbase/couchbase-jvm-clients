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

package com.couchbase.client.java;

import com.couchbase.client.core.error.CouchbaseException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public enum AsyncUtils {
  ;

  /**
   * Helper method to wrap an async call into a blocking one and make sure to
   * convert all checked exceptions into their correct runtime counterparts.
   *
   * @param input the future as input.
   * @param <T> the generic type to return.
   * @return blocks and completes on the given future while converting checked exceptions.
   */
  public static <T> T block(final CompletableFuture<T> input) {
    try {
      return input.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else if (e.getCause() != null && e.getCause() instanceof TimeoutException) {
        throw new RuntimeException(e.getCause());
      } else {
        throw new RuntimeException(e);
      }
    }
  }

}
