/*
 * Copyright (c) 2024 Couchbase, Inc.
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

// CHECKSTYLE:OFF IllegalImport - Allow usage of jctools classes

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.annotation.UsedBy;
import com.couchbase.client.core.deps.org.jctools.queues.MpscArrayQueue;
import java.util.Queue;

import static com.couchbase.client.core.annotation.UsedBy.Project.QUARKUS_COUCHBASE;

@Stability.Internal
public class NativeImageHelper {

  private NativeImageHelper() {
  }

  /**
   * This static factory method has been added in order to help the substitutions
   * for native image compatibility in the Quarkus extension.
   * In the extension, this is substituted for an MpscAtomicUnpaddedArrayQueue,
   * a slightly less performant but memory safe variant.
   * See {@link com.couchbase.client.core.deps.org.jctools.queues.MpscArrayQueue} and {@link com.couchbase.client.core.deps.org.jctools.queues.atomic.unpadded.MpscAtomicUnpaddedArrayQueue}.
   * @param capacity The capacity of the queue
   * @return A new MpscArrayQueue.
   * @param <T> The type held by the queue.
   */
  @UsedBy(QUARKUS_COUCHBASE)
  public static <T> Queue<T> createMpscArrayQueue(int capacity) {
    return new MpscArrayQueue<>(capacity);
  }
}
