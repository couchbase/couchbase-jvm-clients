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
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.couchbase.client.core.util.CbStrings.nullToEmpty;

/**
 * "Squashes" notifications, retaining only the highest revision for each bucket (plus global).
 * <p>
 * Note this is very regularly reset - it's purely intended to debounce when multiple configs arrive in quick succession.
 */
@NullMarked
@Stability.Internal
public class TopologyChangeNotificationBuffer {
  private final ConcurrentMap<String, TopologyRevision> bucketNameToRevision = new ConcurrentHashMap<>();
  private static final Logger logger = LoggerFactory.getLogger(TopologyChangeNotificationBuffer.class);

  /**
   * Returns true if the given revision was added to the buffer,
   * or false if it was not newer than the existing entry.
   *
   * @param bucketName empty means global
   * @param origin where the topology arrived from, for debugging
   */
  public boolean putIfNewer(
    String bucketName,
    TopologyRevision revision,
    @Nullable HostAndPort origin
  ) {
    // All current callers of this method already use nullToEmpty on bucketName first.  But to protect against future mishaps:
    String nonNullBucketName = key(bucketName);
    TopologyRevision updatedRevision = bucketNameToRevision.compute(
      nonNullBucketName,
      (k, existing) -> {
        boolean willUse = existing == null || revision.newerThan(existing);
        logger.debug("Determining if new topology {} for '{}'{} exceeds existing {}: {}",
          revision,
          nonNullBucketName.isEmpty() ? "global" : nonNullBucketName,
          origin == null ? "" : " from " + origin,
          existing,
          willUse);
        return willUse ? revision : existing;
      }
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
