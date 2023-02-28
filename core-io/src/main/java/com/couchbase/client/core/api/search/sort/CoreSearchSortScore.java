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
import com.couchbase.client.protostellar.search.v1.ScoreSorting;
import com.couchbase.client.protostellar.search.v1.Sorting;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreSearchSortScore extends CoreSearchSort {

    public CoreSearchSortScore(@Nullable Boolean descending) {
        super(descending);
    }

    @Override
    protected String identifier() {
        return "score";
    }

    @Override
    public Sorting asProtostellar() {
        Sorting.Builder builder = Sorting.newBuilder();

        if (descending != null) {
            builder.setScoreSorting(ScoreSorting.newBuilder().setDescending(descending));
        }

        return builder.build();
    }
}
