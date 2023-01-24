/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.classic.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.api.query.CoreQueryMetrics;
import com.couchbase.client.core.api.query.CoreQueryStatus;
import com.couchbase.client.core.api.query.CoreQueryWarning;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Stability.Internal
public class ClassicCoreQueryMetaData extends CoreQueryMetaData {

  private final QueryChunkHeader header;
  private final QueryChunkTrailer trailer;

  @Stability.Internal
  private ClassicCoreQueryMetaData(QueryChunkHeader header, QueryChunkTrailer trailer) {
    this.header = header;
    this.trailer = trailer;
  }

  @Stability.Internal
  public static ClassicCoreQueryMetaData from(final QueryChunkHeader header, final QueryChunkTrailer trailer) {
    return new ClassicCoreQueryMetaData(header, trailer);
  }

  public String requestId() {
    return header.requestId();
  }

  public String clientContextId() {
    return header.clientContextId().orElse("");
  }

  public CoreQueryStatus status() {
    return CoreQueryStatus.from(trailer.status());
  }

  public Optional<byte[]> signature() {
    return header.signature();
  }

  public Optional<byte[]> profile() {
    return trailer.profile();
  }

  public Optional<CoreQueryMetrics> metrics() {
    return trailer.metrics().map(ClassicCoreQueryMetrics::new);
  }

  public List<CoreQueryWarning> warnings() {
    return this.trailer.warnings().map(warnings ->
        ErrorCodeAndMessage.fromJsonArray(warnings).stream().map(CoreQueryWarning::new).collect(Collectors.toList())
    ).orElse(Collections.emptyList());
  }

  @Override
  public String toString() {
    return "CoreQueryMetaDataClassic{" +
        "header=" + header +
        ", trailer=" + trailer +
        '}';
  }
}
