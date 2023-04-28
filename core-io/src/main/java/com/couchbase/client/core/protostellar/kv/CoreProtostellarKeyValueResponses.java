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
import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreExistsResult;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.api.kv.CoreSubdocMutateCommand;
import com.couchbase.client.core.api.kv.CoreSubdocMutateResult;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.protostellar.CoreProtostellarErrorHandlingUtil;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.protostellar.kv.v1.ExistsResponse;
import com.couchbase.client.protostellar.kv.v1.GetAndLockResponse;
import com.couchbase.client.protostellar.kv.v1.GetAndTouchResponse;
import com.couchbase.client.protostellar.kv.v1.GetResponse;
import com.couchbase.client.protostellar.kv.v1.InsertResponse;
import com.couchbase.client.protostellar.kv.v1.LookupInResponse;
import com.couchbase.client.protostellar.kv.v1.MutateInResponse;
import com.couchbase.client.protostellar.kv.v1.RemoveResponse;
import com.couchbase.client.protostellar.kv.v1.ReplaceResponse;
import com.couchbase.client.protostellar.kv.v1.TouchResponse;
import com.couchbase.client.protostellar.kv.v1.UnlockResponse;
import com.couchbase.client.protostellar.kv.v1.UpsertResponse;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

  protected static CoreMutationResult convertMutationResult(CoreKeyspace keyspace, String key, long cas, @Nullable com.couchbase.client.protostellar.kv.v1.MutationToken mt) {
    Optional<MutationToken> mutationToken = convertMutationToken(mt);
    return new CoreMutationResult(null, keyspace, key, cas, mutationToken);
  }

  private static Optional<MutationToken> convertMutationToken(com.couchbase.client.protostellar.kv.v1.MutationToken mt) {
    Optional<MutationToken> mutationToken;
    if (mt != null) {
      mutationToken = Optional.of(new MutationToken((short) mt.getVbucketId(), mt.getVbucketId(), mt.getSeqNo(), mt.getBucketName()));
    } else {
      mutationToken = Optional.empty();
    }
    return mutationToken;
  }

  public static CoreGetResult convertResponse(CoreKeyspace keyspace, String key, GetResponse response) {
    return new CoreGetResult(CoreKvResponseMetadata.NONE,
      keyspace,
      key,
      response.getContent().toByteArray(),
      response.getContentFlags(),
      response.getCas(),
      CoreProtostellarUtil.convertExpiry(response.hasExpiry(), response.getExpiry()),
      false);
  }

  public static CoreGetResult convertResponse(CoreKeyspace keyspace, String key, GetAndLockResponse response) {
    return new CoreGetResult(CoreKvResponseMetadata.NONE,
      keyspace,
      key,
      response.getContent().toByteArray(),
      response.getContentFlags(),
      response.getCas(),
      CoreProtostellarUtil.convertExpiry(response.hasExpiry(), response.getExpiry()),
      false);
  }

  public static CoreGetResult convertResponse(CoreKeyspace keyspace, String key, GetAndTouchResponse response) {
    return new CoreGetResult(CoreKvResponseMetadata.NONE,
      keyspace,
      key,
      response.getContent().toByteArray(),
      response.getContentFlags(),
      response.getCas(),
      CoreProtostellarUtil.convertExpiry(response.hasExpiry(), response.getExpiry()),
      false);
  }

  public static CoreSubdocMutateResult convertResponse(CoreKeyspace keyspace, String key, MutateInResponse response, List<CoreSubdocMutateCommand> specs) {
    List<SubDocumentField> responses = new ArrayList<>(response.getSpecsCount());

    for (int i = 0; i < response.getSpecsList().size(); i++) {
      CoreSubdocMutateCommand original = specs.get(i);
      MutateInResponse.Spec resp = response.getSpecsList().get(i);

      responses.add(new SubDocumentField(SubDocumentOpResponseStatus.SUCCESS,
        Optional.empty(),
        resp.getContent().toByteArray(),
        original.path(),
        original.type()));
    }

    return new CoreSubdocMutateResult(keyspace,
      key,
      null,
      response.getCas(),
      response.hasMutationToken() ? convertMutationToken(response.getMutationToken()) : Optional.empty(),
      responses);
  }

  public static CoreSubdocGetResult convertResponse(CoreProtostellar core, ProtostellarRequest<?> request, CoreKeyspace keyspace, String key, LookupInResponse response, List<CoreSubdocGetCommand> specs) {
    List<SubDocumentField> responses = new ArrayList<>(response.getSpecsCount());

    for (int i = 0; i < response.getSpecsList().size(); i++) {
      CoreSubdocGetCommand original = specs.get(i);
      LookupInResponse.Spec resp = response.getSpecsList().get(i);
      com.couchbase.client.core.deps.com.google.rpc.Status status = resp.getStatus();
      ProtostellarRequestBehaviour behaviour = CoreProtostellarErrorHandlingUtil.convertStatus(core, request, null, status);

      boolean isFailedExists = original.type() == SubdocCommandType.EXISTS && resp.getContent().toStringUtf8().equalsIgnoreCase("false");

      CouchbaseException exception = behaviour.exception() == null
              ? (isFailedExists ? new PathNotFoundException(null) : null)
              : behaviour.exception() instanceof CouchbaseException ? (CouchbaseException) behaviour.exception()
              : new CouchbaseException(behaviour.exception());

      SubDocumentOpResponseStatus stat = exception == null ? SubDocumentOpResponseStatus.SUCCESS : SubDocumentOpResponseStatus.UNKNOWN;

      responses.add(new SubDocumentField(stat,
              Optional.ofNullable(exception),
              resp.getContent().toByteArray(),
              original.path(),
              original.type()));
    }

    return new CoreSubdocGetResult(keyspace,
            key,
            null,
            responses,
            response.getCas(),
            // Protostellar does not indicate whether the document was a tombstone - but, that is only used by transactions,
            // which will use Protostellar directly.
            false
            );
  }
}
