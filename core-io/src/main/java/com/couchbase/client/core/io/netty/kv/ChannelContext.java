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

import com.couchbase.client.core.env.CompressionConfig;

/**
 * Holds context to encode KV operations based on what got negotiated in the channel.
 *
 * @since 2.0.0
 */
public class ChannelContext {

  private final String bucket;
  private final CompressionConfig compression;
  private final boolean collections;
  private final boolean mutationTokensEnabled;

  public ChannelContext(CompressionConfig compression, boolean collections, boolean mutationTokens, String bucket) {
    this.compression = compression;
    this.collections = collections;
    this.mutationTokensEnabled = mutationTokens;
    this.bucket = bucket;
  }

  public boolean collectionsEnabled() {
    return collections;
  }

  public boolean compressionEnabled() {
    return compression != null;
  }

  public boolean mutationTokensEnabled() {
    return mutationTokensEnabled;
  }

  public CompressionConfig compressionConfig() {
    return compression;
  }

  public String bucket() {
    return bucket;
  }

}
