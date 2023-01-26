/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar.kv;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreExistsResult;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.protostellar.kv.v1.ExistsResponse;
import com.couchbase.client.protostellar.kv.v1.GetResponse;
import com.couchbase.client.protostellar.kv.v1.InsertResponse;
import com.couchbase.client.protostellar.kv.v1.RemoveResponse;
import com.couchbase.client.protostellar.kv.v1.ReplaceResponse;
import com.couchbase.client.protostellar.kv.v1.TouchResponse;
import com.couchbase.client.protostellar.kv.v1.UnlockResponse;
import com.couchbase.client.protostellar.kv.v1.UpsertResponse;
import reactor.util.annotation.Nullable;

import java.util.Optional;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertToFlags;

/**
 * For converting Protostellar GRPC KV responses.
 */
@Stability.Internal
public class CoreProtostellarKeyValueResponses {
  private CoreProtostellarKeyValueResponses() {}

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, InsertResponse response) {
    return convertMutationResult(keyspace, key, response.getCas(), response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, RemoveResponse response) {
    return convertMutationResult(keyspace, key, response.getCas(), response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, ReplaceResponse response) {
    return convertMutationResult(keyspace, key, response.getCas(), response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, UpsertResponse response) {
    return convertMutationResult(keyspace, key, response.getCas(), response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, TouchResponse response) {
    return convertMutationResult(keyspace, key, response.getCas(), response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, UnlockResponse response) {
    return convertMutationResult(keyspace, key, 0, null);
  }

  public static CoreExistsResult convertResponse(CoreKeyspace keyspace, String key, ExistsResponse response) {
    return new CoreExistsResult(null, keyspace, key, response.getCas(), response.getResult());
  }

  private static CoreMutationResult convertMutationResult(CoreKeyspace keyspace, String key, long cas, @Nullable com.couchbase.client.protostellar.kv.v1.MutationToken mt) {
    Optional<MutationToken> mutationToken;
    if (mt != null) {
      mutationToken = Optional.of(new MutationToken((short) mt.getVbucketId(), mt.getVbucketId(), mt.getSeqNo(), mt.getBucketName()));
    } else {
      mutationToken = Optional.empty();
    }
    return new CoreMutationResult(null, keyspace, key, cas, mutationToken);
  }

  public static CoreGetResult convertResponse(CoreKeyspace keyspace, String key, GetResponse response) {
    return new CoreGetResult(CoreKvResponseMetadata.NONE,
      keyspace,
      key,
      response.getContent().toByteArray(),
      convertToFlags(response.getContentType()),
      response.getCas(),
      CoreProtostellarUtil.convertExpiry(response.hasExpiry(), response.getExpiry()),
      false);
  }
}
