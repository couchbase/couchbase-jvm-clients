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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.channel.ChannelId;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.io.CollectionMap;
import reactor.util.annotation.Nullable;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Holds context to encode KV operations based on what got negotiated in the channel.
 *
 * @since 2.0.0
 */
public class KeyValueChannelContext {

  private final Optional<String> bucket;
  private final CompressionConfig compression;
  private final boolean collections;
  private final boolean mutationTokensEnabled;
  private final boolean syncReplication;
  private final boolean vattrEnabled;
  private final boolean altRequest;
  private final boolean createAsDeleted;
  private final boolean preserveTtl;
  private final CollectionMap collectionMap;
  private final ChannelId channelId;

  @Stability.Internal
  public KeyValueChannelContext(
    @Nullable final CompressionConfig compression,
    final Optional<String> bucket,
    final CollectionMap collectionMap,
    @Nullable final ChannelId channelId,
    final Set<ServerFeature> features
  ) {
    this.compression = compression;
    this.bucket = requireNonNull(bucket); // may be absent, but the Optional itself must not be null
    this.collectionMap = requireNonNull(collectionMap);
    this.channelId = channelId;

    this.collections = features.contains(ServerFeature.COLLECTIONS);
    this.mutationTokensEnabled = features.contains(ServerFeature.MUTATION_SEQNO);
    this.syncReplication = features.contains(ServerFeature.SYNC_REPLICATION);
    this.altRequest = features.contains(ServerFeature.ALT_REQUEST);
    this.vattrEnabled = features.contains(ServerFeature.VATTR);
    this.createAsDeleted = features.contains(ServerFeature.CREATE_AS_DELETED);
    this.preserveTtl = features.contains(ServerFeature.PRESERVE_TTL);

    if (syncReplication && !altRequest) {
      throw new IllegalArgumentException("If Synchronous Replication is enabled, the server also " +
        "must negotiate Alternate Requests. This is a bug! - please report.");
    }
  }

  public boolean collectionsEnabled() {
    return collections;
  }

  public boolean compressionEnabled() {
    return compression != null;
  }

  @Nullable
  public CompressionConfig compressionConfig() {
    return compression;
  }

  public boolean mutationTokensEnabled() {
    return mutationTokensEnabled;
  }

  public boolean syncReplicationEnabled() {
    return syncReplication;
  }

  public boolean vattrEnabled() {
    return vattrEnabled;
  }

  public boolean alternateRequestEnabled() {
    return altRequest;
  }

  public CollectionMap collectionMap() {
    return collectionMap;
  }

  @Nullable
  public ChannelId channelId() {
    return channelId;
  }

  public boolean createAsDeleted() {
    return createAsDeleted;
  }

  public boolean preserveTtl() {
    return preserveTtl;
  }

  /**
   * The name of the bucket.
   */
  public Optional<String> bucket() {
    return bucket;
  }

}
