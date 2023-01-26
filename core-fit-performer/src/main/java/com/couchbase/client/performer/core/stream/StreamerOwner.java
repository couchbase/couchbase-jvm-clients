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

import com.couchbase.client.protocol.streams.CancelRequest;
import com.couchbase.client.protocol.streams.RequestItemsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Streamers are bound to the RPC that created them.
 * <p>
 * There is one StreamerOwner per RPC, and it owns a bunch of Streamers.
 */
public class StreamerOwner extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(StreamerOwner.class);
    private final List<Streamer> streamers = new ArrayList<>();

    public void addAndStart(Streamer sr) {
        synchronized (streamers) {
            streamers.add(sr);
        }
        sr.start();

        // This is for FluxStreamer: we can't do anything with the stream until the subscription has been asynchronously
        // registered.
        while (!sr.isCreated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void waitForAllStreamsFromRun(String runId) {
        List<Streamer> streamersForRun;
        synchronized (streamers) {
            streamersForRun = streamers.stream().filter(streamer -> streamer.runId().equals(runId)).toList();
        }

        logger.info("Waiting for {} streamers from run {}", streamersForRun.size(), runId);

        streamersForRun.forEach(streamer -> {
            try {
                streamer.join();
                synchronized (streamers) {
                    streamers.removeIf(s -> s.runId().equals(streamer.runId()));
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        logger.info("All streamers finished for run, {} remaining streamers", streamers.size());
    }

    private Streamer getStream(String streamId) {
        Streamer streamer;
        synchronized (streamers) {
            var streamerOpt = streamers.stream().filter(v -> v.streamId().equals(streamId)).findFirst();

            if (streamerOpt.isEmpty()) {
                throw new RuntimeException("Could not find stream " + streamId);
            }

            streamer = streamerOpt.get();
        }

        return streamer;
    }

    public void cancel(CancelRequest request) {
        logger.info("Cancelling stream {}", request.getStreamId());
        getStream(request.getStreamId()).cancel();
    }

    public void requestItems(RequestItemsRequest request) {
        logger.info("Requesting {} more items from stream {}", request.getNumItems(), request.getStreamId());
        getStream(request.getStreamId()).requestItems(request);
    }
}
