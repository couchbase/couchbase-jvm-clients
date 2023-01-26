/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Stability.Internal
public class CoreAsyncUtils {
  private CoreAsyncUtils() {
    throw new AssertionError("not instantiable");
  }

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
      final Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        // Rethrow the cause but first adjust the stack trace to point HERE instead of
        // the thread where the exception was actually thrown, otherwise the stack trace
        // does not include the context of the blocking call.
        // Preserve the original async stack trace as a suppressed exception.
        Exception suppressed = new Exception(
            "The above exception was originally thrown by another thread at the following location.");
        suppressed.setStackTrace(cause.getStackTrace());
        cause.fillInStackTrace();
        cause.addSuppressed(suppressed);
        throw (RuntimeException) cause;
      }
      if (cause instanceof TimeoutException) {
        throw new RuntimeException(cause);
      }
      throw new RuntimeException(e);
    }
  }
}
