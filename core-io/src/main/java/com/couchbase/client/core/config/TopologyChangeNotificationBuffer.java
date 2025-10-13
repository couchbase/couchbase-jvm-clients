/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.topology.TopologyRevision;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.couchbase.client.core.util.CbStrings.nullToEmpty;

/**
 * "Squashes" notifications, retaining only the highest revision for each bucket (plus global).
 */
@NullMarked
@Stability.Internal
public class TopologyChangeNotificationBuffer {
  private final ConcurrentMap<String, TopologyRevision> bucketNameToRevision = new ConcurrentHashMap<>();

  /**
   * Returns true if the given revision was added to the buffer,
   * or false if it was not newer than the existing entry.
   *
   * @param bucketName null means global
   */
  public boolean putIfNewer(
    @Nullable String bucketName,
    TopologyRevision revision
  ) {
    TopologyRevision updatedRevision = bucketNameToRevision.compute(
      key(bucketName),
      (k, existing) ->
        existing == null || revision.newerThan(existing)
          ? revision
          : existing
    );

    return revision == updatedRevision;
  }

  /**
   * @param bucketName null means global
   */
  public @Nullable TopologyRevision remove(@Nullable String bucketName) {
    return bucketNameToRevision.remove(key(bucketName));
  }

  private static String key(@Nullable String bucketName) {
    return nullToEmpty(bucketName);
  }

  @Override
  public String toString() {
    return "TopologyChangeNotificationBuffer{" +
      "bucketNameToRevision=" + bucketNameToRevision +
      '}';
  }
}
