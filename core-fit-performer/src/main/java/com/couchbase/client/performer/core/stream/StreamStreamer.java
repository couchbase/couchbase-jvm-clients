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
package com.couchbase.client.performer.core.stream;

import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.streams.Config;

import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Streams back a java.util.stream.Stream.
 */
public class StreamStreamer<T> extends IteratorBasedStreamer<T> {
    private final Iterator<T> iterator;

    public StreamStreamer(Stream<T> results,
                          PerRun perRun,
                          String streamId,
                          Config streamConfig,
                          Function<T, Result> convertResult,
                          Function<Throwable, com.couchbase.client.protocol.shared.Exception> convertException) {
        super(perRun, streamId, streamConfig, convertResult, convertException);
        this.iterator = results.iterator();
    }

    @Override
    protected T next() {
        return iterator.next();
    }

    @Override
    protected boolean hasNext() {
        return iterator.hasNext();
    }
}
