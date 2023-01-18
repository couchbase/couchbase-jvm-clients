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
import com.couchbase.client.protocol.streams.RequestItemsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Abstracts over a stream of results being returned.  Intended to be abstract enough to handle streaming collections
 * from all the JVM implementations.
 */
abstract public class Streamer<T> extends Thread {
    protected static final Logger logger = LoggerFactory.getLogger(Streamer.class);
    protected final PerRun perRun;
    protected final String streamId;
    protected final Config streamConfig;
    protected final Function<T, Result> convertResult;
    protected final Function<Throwable, com.couchbase.client.protocol.shared.Exception> convertException;
    protected final AtomicInteger streamed = new AtomicInteger(0);

    public Streamer(PerRun perRun,
                    String streamId,
                    Config streamConfig,
                    Function<T, Result> convertResult,
                    Function<Throwable, com.couchbase.client.protocol.shared.Exception> convertException) {
        this.perRun = perRun;
        this.streamId = streamId;
        this.streamConfig = streamConfig;
        this.convertResult = convertResult;
        this.convertException = convertException;
    }

    public String streamId() {
        return streamId;
    }

    public String runId() {
        return perRun.runId();
    }

    abstract public void cancel();

    abstract public void requestItems(RequestItemsRequest request);

    abstract public boolean isCreated();
}
