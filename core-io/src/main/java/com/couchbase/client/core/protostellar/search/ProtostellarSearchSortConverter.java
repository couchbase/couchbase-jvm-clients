/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.protostellar.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.sort.CoreSearchSortConverter;
import com.couchbase.client.core.api.search.sort.CoreSearchSortField;
import com.couchbase.client.core.api.search.sort.CoreSearchSortGeoDistance;
import com.couchbase.client.core.api.search.sort.CoreSearchSortId;
import com.couchbase.client.core.api.search.sort.CoreSearchSortScore;
import com.couchbase.client.core.api.search.sort.CoreSearchSortString;
import com.couchbase.client.protostellar.search.v1.FieldSorting;
import com.couchbase.client.protostellar.search.v1.GeoDistanceSorting;
import com.couchbase.client.protostellar.search.v1.IdSorting;
import com.couchbase.client.protostellar.search.v1.ScoreSorting;
import com.couchbase.client.protostellar.search.v1.Sorting;
import org.jspecify.annotations.NullMarked;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.toLatLng;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static com.couchbase.client.core.protostellar.search.ProtostellarSearchQueryConverter.ifNotNull;

@Stability.Internal
@NullMarked
public class ProtostellarSearchSortConverter implements CoreSearchSortConverter<Sorting> {

  public static final ProtostellarSearchSortConverter instance = new ProtostellarSearchSortConverter();

  private ProtostellarSearchSortConverter() {
  }

  @Override
  public Sorting convert(CoreSearchSortString core) {
    // Requires ING-381
    throw unsupportedInProtostellar("specifying FTS sort order using a string");
  }

  @Override
  public Sorting convert(CoreSearchSortField core) {
    FieldSorting.Builder builder = FieldSorting.newBuilder()
      .setField(core.field);

    ifNotNull(core.missing, it -> builder.setMissing(it.value()));
    ifNotNull(core.mode, it -> builder.setMode(it.value()));
    ifNotNull(core.type, it -> builder.setType(it.value()));

    return Sorting.newBuilder().setFieldSorting(builder).build();
  }

  @Override
  public Sorting convert(CoreSearchSortGeoDistance core) {
    GeoDistanceSorting.Builder builder = GeoDistanceSorting.newBuilder()
      .setField(core.field)
      .setCenter(toLatLng(core.location));

    ifNotNull(core.descending, builder::setDescending);
    ifNotNull(core.unit, it -> builder.setUnit(it.identifier()));

    return Sorting.newBuilder().setGeoDistanceSorting(builder).build();
  }

  @Override
  public Sorting convert(CoreSearchSortId core) {
    IdSorting.Builder builder = IdSorting.newBuilder();

    ifNotNull(core.descending, builder::setDescending);

    return Sorting.newBuilder().setIdSorting(builder).build();
  }

  @Override
  public Sorting convert(CoreSearchSortScore core) {
    ScoreSorting.Builder builder = ScoreSorting.newBuilder();

    ifNotNull(core.descending, builder::setDescending);

    return Sorting.newBuilder().setScoreSorting(builder).build();
  }
}
