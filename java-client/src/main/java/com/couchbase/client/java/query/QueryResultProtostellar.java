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
package com.couchbase.client.java.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.protostellar.query.v1.QueryResponse;

import java.util.List;
import java.util.stream.Collectors;

@Stability.Volatile
public class QueryResultProtostellar extends QueryResult {
  private final List<QueryResponse> responses;

  public QueryResultProtostellar(List<QueryResponse> responses, JsonSerializer serializer) {
    super(serializer);
    this.responses = responses;
  }

  @Override
  public <T> List<T> rowsAs(Class<T> target) {
    return responses.stream()
      .flatMap(response -> response.getRowsList().stream())
      .map(row -> serializer.deserialize(target, row.toByteArray()))
      .collect(Collectors.toList());
  }

  @Override
  public <T> List<T> rowsAs(TypeRef<T> target) {
    return responses.stream()
      .flatMap(response -> response.getRowsList().stream())
      .map(row -> serializer.deserialize(target, row.toByteArray()))
      .collect(Collectors.toList());
  }

  @Override
  public QueryMetaData metaData() {
    return responses.stream()
      .filter(v -> v.hasMetaData())
      .map(v -> new QueryMetaDataProtostellar(v.getMetaData()))
      .findFirst()
      .get();
  }
}
