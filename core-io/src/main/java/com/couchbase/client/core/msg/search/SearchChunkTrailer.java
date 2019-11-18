/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.msg.search;

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;

public class SearchChunkTrailer implements ChunkTrailer {

    private final long totalRows;
    private final double maxScore;
    private final long took;
    private final byte[] facets;

    public SearchChunkTrailer(long totalRows, double maxScore, long took, byte[] facets) {
        this.totalRows = totalRows;
        this.maxScore = maxScore;
        this.took = took;
        this.facets = facets;
    }

    public long totalRows() {
        return totalRows;
    }

    public double maxScore() {
        return maxScore;
    }

    public long took() {
        return took;
    }

    public byte[] facets() {
        return facets;
    }

    @Override
    public String toString() {
        return "SearchChunkTrailer{" +
          "totalRows=" + totalRows +
          ", maxScore=" + maxScore +
          ", took=" + took +
          ", facets=" + (facets != null ? new String(facets, CharsetUtil.UTF_8) : "null") +
          '}';
    }
}
