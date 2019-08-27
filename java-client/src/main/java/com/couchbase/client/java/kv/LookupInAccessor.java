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
import com.couchbase.client.core.error.DefaultErrorUtil;
import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.core.msg.kv.SubdocField;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LookupInAccessor {

  public static CompletableFuture<LookupInResult> lookupInAccessor(final String id,
                                                                   final Core core,
                                                                   final SubdocGetRequest request,
                                                                   final boolean withExpiration) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        switch (response.status()) {
          case SUCCESS:
              if (withExpiration) {
                  ArrayList<SubdocField> values = new ArrayList<>(response.values().size() - 1);
                  byte[] exptime = null;

                  for (SubdocField value : response.values()) {
                      if (value.path().equals(GetAccessor.EXPIRATION_MACRO)) {
                          exptime = value.value();
                      }
                      else {
                          values.add(value);
                      }
                  }

                  Optional<Duration> expiration = exptime == null
                          ? Optional.empty()
                          : Optional.of(Duration.ofSeconds(Long.parseLong(new String(exptime, UTF_8))));

                  return new LookupInResult(values, response.cas(), expiration);
              }
              else {
                  return new LookupInResult(response.values(), response.cas(), Optional.empty());
              }
          case SUBDOC_FAILURE:
            throw response.error().orElse(new SubDocumentException("Unknown SubDocument failure occurred") {});
            default:
                throw DefaultErrorUtil.defaultErrorForStatus(id, response.status());
        }
      });
  }
}
