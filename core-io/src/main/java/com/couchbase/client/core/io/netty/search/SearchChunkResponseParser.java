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

package com.couchbase.client.core.io.netty.search;

import com.couchbase.client.core.error.SearchServiceException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.search.SearchChunkHeader;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.util.yasjl.JsonPointer;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SearchChunkResponseParser
        extends BaseChunkResponseParser<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer> {

    private byte[] status;
    private byte[] error;

    private long totalHits;
    private double maxScore;
    private long took;

    @Override
    protected ByteBufJsonParser initParser() {
        return new ByteBufJsonParser(new JsonPointer[] {
            new JsonPointer("/status", value -> status = value),
            new JsonPointer("/error", value -> {
                error = value;
                failRows(new SearchServiceException(error));
            }),
            new JsonPointer("/hits/-", value -> emitRow(new SearchChunkRow(value))),
            new JsonPointer("/total_hits", value -> totalHits = Mapper.decodeInto(value, Long.class)),
            new JsonPointer("/max_score", value -> maxScore = Mapper.decodeInto(value, Double.class)),
            new JsonPointer("/took", value -> took = Mapper.decodeInto(value, Long.class))
        });
    }

    @Override
    protected void resetState() {
        status = null;
        totalHits = 0;
        maxScore = 0.0;
        took = 0;
    }

    @Override
    public Optional<SearchChunkHeader> header() {
        if (status != null) {
            return Optional.of(new SearchChunkHeader(status));
        }
        return Optional.empty();    }

    @Override
    public Optional<Throwable> error() {
        if (error == null) {
            return Optional.empty();
        } else {
            return Optional.of(new SearchServiceException(error));
        }
    }

    @Override
    public void signalComplete() {
        completeRows();
        completeTrailer(new SearchChunkTrailer(totalHits, maxScore, took));
    }
}
