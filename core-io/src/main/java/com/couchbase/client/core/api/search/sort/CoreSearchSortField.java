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
package com.couchbase.client.core.api.search.sort;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.protostellar.search.v1.FieldSorting;
import com.couchbase.client.protostellar.search.v1.Sorting;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreSearchSortField extends CoreSearchSort {

    private final String field;

    private final @Nullable CoreSearchFieldType type;
    private final @Nullable CoreSearchFieldMode mode;
    private final @Nullable CoreSearchFieldMissing missing;

    public CoreSearchSortField(String field,
                               @Nullable CoreSearchFieldType type,
                               @Nullable CoreSearchFieldMode mode,
                               @Nullable CoreSearchFieldMissing missing,
                               boolean descending) {
        super(descending);
        this.field = notNull(field, "Field");
        this.type = type;
        this.mode = mode;
        this.missing = missing;
    }

    @Override
    protected String identifier() {
        return "field";
    }

    @Override
    protected void injectParams(ObjectNode queryJson) {
        super.injectParams(queryJson);

        queryJson.put("field", field);

        if (type != null) {
            queryJson.put("type", type.value());
        }
        if (mode != null) {
            queryJson.put("mode", mode.value());
        }
        if (missing != null) {
            queryJson.put("missing", missing.value());
        }
    }

    @Override
    public Sorting asProtostellar() {
        FieldSorting.Builder builder = FieldSorting.newBuilder()
                .setField(field)
                .setDescending(descending);

        if (missing != null) {
            builder.setMissing(missing.value());
        }

        if (mode != null) {
            builder.setMode(mode.value());
        }

        if (type != null) {
            builder.setType(type.value());
        }

        return Sorting.newBuilder().setFieldSorting(builder).build();
    }
}
