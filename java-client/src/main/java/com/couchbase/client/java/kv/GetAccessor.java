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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.projections.ProjectionsApplier;
import com.couchbase.client.java.codec.Transcoder;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
import static java.nio.charset.StandardCharsets.UTF_8;

@Stability.Internal
public enum GetAccessor {
  ;

  /**
   * Takes a {@link GetRequest} and dispatches, converts and returns the result.
   *
   * @param core the core reference to dispatch into.
   * @param request the request to dispatch and convert once a response arrives.
   * @param transcoder the transcoder used to decode the response body.
   * @return a {@link CompletableFuture} once the document is fetched and decoded.
   */
  public static CompletableFuture<GetResult> get(final Core core, final GetRequest request, final Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new GetResult(response.content(), response.flags(), response.cas(), Optional.empty(), transcoder);
        }
        throw keyValueStatusToException(request, response);
      })
      .whenComplete((t, e) -> request.context().logicallyComplete());
  }

  /**
   * Takes a {@link GetAndLockRequest} and dispatches, converts and returns the result.
   *
   * @param core the core reference to dispatch into.
   * @param request the request to dispatch and convert once a response arrives.
   * @param transcoder the transcoder used to decode the response body.
   * @return a {@link CompletableFuture} once the document is fetched and decoded.
   */
  public static CompletableFuture<GetResult> getAndLock(final Core core, final GetAndLockRequest request,
                                                        final Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new GetResult(response.content(), response.flags(), response.cas(), Optional.empty(), transcoder);
        }
        throw keyValueStatusToException(request, response);
      })
      .whenComplete((t, e) -> request.context().logicallyComplete());
  }

  public static CompletableFuture<GetResult> getAndTouch(final Core core, final GetAndTouchRequest request,
                                                         final Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new GetResult(response.content(), response.flags(), response.cas(), Optional.empty(), transcoder);
        }
        throw keyValueStatusToException(request, response);
      })
      .whenComplete((t, e) -> request.context().logicallyComplete());
  }

  public static CompletableFuture<GetResult> subdocGet(final Core core, final SubdocGetRequest request,
                                                       final Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success() || response.status() == ResponseStatus.SUBDOC_FAILURE) {
          return parseSubdocGet(response, transcoder);
        }
        throw keyValueStatusToException(request, response);
      }).whenComplete((t, e) -> request.context().logicallyComplete());
  }

  private static GetResult parseSubdocGet(final SubdocGetResponse response, final Transcoder transcoder) {
    if (response.error().isPresent()) {
      throw response.error().get();
    }

    long cas = response.cas();

    byte[] exptime = null;
    byte[] content = null;
    byte[] flags = null;

    for (SubDocumentField value : response.values()) {
      if (value != null) {
        if (LookupInMacro.EXPIRY_TIME.equals(value.path())) {
          exptime = value.value();
        } else if (LookupInMacro.FLAGS.equals(value.path())) {
          flags = value.value();
        } else if (value.path().isEmpty()) {
          content = value.value();
        }
      }
    }

    int convertedFlags = flags == null ? CodecFlags.JSON_COMPAT_FLAGS : Integer.parseInt(new String(flags, UTF_8));

    if (content == null) {
      try {
        content = ProjectionsApplier.reconstructDocument(response);
      } catch (Exception e) {
        throw new CouchbaseException("Unexpected Exception while decoding Sub-Document get", e);
      }
    }

    Optional<Instant> expiration = exptime == null
      ? Optional.empty()
      : Optional.of(Instant.ofEpochSecond(Long.parseLong(new String(exptime, UTF_8))));

    return new GetResult(content, convertedFlags, cas, expiration, transcoder);
  }

}
