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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.Validators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Objects.requireNonNull;

/**
 * Helper class that language-specific clients may use to implement {@code MutationState}.
 * <p>
 * Holds a set of tokens. Guarantees no two tokens are for the same bucket and partition.
 * Knows how to export the tokens for N1QL and FTS queries.
 * <p>
 * Thread-safe.
 */
@Stability.Internal
public class MutationTokenAggregator implements Iterable<MutationToken> {

  /**
   * Key for the token map
   */
  private static class BucketAndPartition {
    private final String bucket;
    private final short partition;

    public BucketAndPartition(String bucket, short partition) {
      this.bucket = requireNonNull(bucket);
      this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      BucketAndPartition key = (BucketAndPartition) o;
      return partition == key.partition && bucket.equals(key.bucket);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bucket, partition);
    }
  }

  /**
   * The tokens, indexed by bucket name and partition.
   */
  private final ConcurrentHashMap<BucketAndPartition, MutationToken> tokens = new ConcurrentHashMap<>();

  @Override
  public Iterator<MutationToken> iterator() {
    return tokens.values().iterator();
  }

  @Override
  public String toString() {
    return tokens.values().toString();
  }

  /**
   * Adds the token to the aggregator, unless there's already a token with the same
   * bucket name and partition and a higher sequence number.
   */
  public void add(MutationToken token) {
    Validators.notNull(token, "token");

    BucketAndPartition key = new BucketAndPartition(token.bucketName(), token.partitionID());
    tokens.compute(key, (bucketAndPartition, exitingToken) -> {
      if (exitingToken == null || Long.compareUnsigned(token.sequenceNumber(), exitingToken.sequenceNumber()) >= 0) {
        return token; // replace existing token
      }
      return exitingToken; // existing token had higher sequence number
    });
  }

  /**
   * Exports the tokens into a universal format.
   * <p>
   * The result can be serialized into a N1QL query,
   * or to sent over the network to a different application/SDK
   * to be reconstructed by {@link #from(Map)}.
   *
   * @return A map containing only Strings and boxed primitives.
   */
  public Map<String, ?> export() {
    // {bucket name -> { vbucket -> [sequence number, vbucketUUID]}
    Map<String, Map<String, List<Object>>> result = new HashMap<>();
    for (MutationToken token : this) {
      Map<String, List<Object>> bucket = result.computeIfAbsent(token.bucketName(), key -> new HashMap<>());
      bucket.put(
          String.valueOf(token.partitionID()),
          listOf(token.sequenceNumber(), String.valueOf(token.partitionUUID())));
    }
    return result;
  }

  /**
   * Exports the tokens into a format recognized by the FTS search engine.
   *
   * @return A map containing only Strings and boxed primitives.
   */
  public Map<String, ?> exportForSearch() {
    Map<String, Long> result = new HashMap<>();
    for (MutationToken token : this) {
      String tokenKey = token.partitionID() + "/" + token.partitionUUID();
      result.put(tokenKey, token.sequenceNumber());
    }
    return result;
  }

  /**
   * Parses the serialized form returned by {@link #export}
   */
  public static MutationTokenAggregator from(Map<String, ?> source) {
    try {
      MutationTokenAggregator state = new MutationTokenAggregator();
      for (String bucketName : source.keySet()) {
        Map<String, List<Object>> bucket = (Map<String, List<Object>>) source.get(bucketName);
        for (String vbid : bucket.keySet()) {
          List<Object> values = bucket.get(vbid);
          state.add(new MutationToken(
              Short.parseShort(vbid),
              Long.parseLong((String) values.get(1)),
              ((Number) values.get(0)).longValue(),
              bucketName
          ));
        }
      }
      return state;
    } catch (Exception ex) {
      throw new IllegalArgumentException("Could not import MutationState from JSON.", ex);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MutationTokenAggregator that = (MutationTokenAggregator) o;
    return tokens.equals(that.tokens);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tokens);
  }
}
