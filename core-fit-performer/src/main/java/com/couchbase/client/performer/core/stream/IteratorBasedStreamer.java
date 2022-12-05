package com.couchbase.client.performer.core.stream;

import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.streams.Config;
import com.couchbase.client.protocol.streams.RequestItemsRequest;

import java.util.function.Function;

/**
 * Many types of streams can be handled with a simple iterator-esque approach.
 */
public abstract class IteratorBasedStreamer<T> extends Streamer<T> {
    protected volatile int demanded = 0;
    protected volatile boolean cancelled = false;

    public IteratorBasedStreamer(PerRun perRun,
                                 String streamId,
                                 Config streamConfig,
                                 Function<T, Result> convertResult,
                                 Function<Throwable, com.couchbase.client.protocol.shared.Exception> convertException) {
        super(perRun, streamId, streamConfig, convertResult, convertException);
    }

    protected abstract T next();

    protected abstract boolean hasNext();

    private void enqueueNext() {
        T next = next();
        Result result = convertResult.apply(next);
        perRun.resultsStream().enqueue(result);
        int streamedNow = streamed.incrementAndGet();
        logger.info("Streamer {} got and enqueued an item, has sent {}", streamId, streamedNow);
    }

    @Override
    public boolean isCreated() {
        return true;
    }

    @Override
    public void run() {
        try {
            logger.info("Streamer {} has started", streamId);

            boolean done = false;

            while (!done && !cancelled) {
                if (streamConfig.hasAutomatically()) {
                    while (hasNext()) {
                        enqueueNext();
                    }
                    done = true;
                } else if (streamConfig.hasOnDemand()) {
                    while (demanded == 0 && !done && !cancelled) {
                        Thread.sleep(10);
                    }
                    if (done || cancelled) {
                        break;
                    }
                    for (int i = 0; i < demanded; i++) {
                        if (hasNext()) {
                            enqueueNext();
                        }
                    }
                    demanded = 0;
                    if (!hasNext()) {
                        done = true;
                    }
                } else {
                    throw new UnsupportedOperationException();
                }

                if (!done) {
                    Thread.sleep(10);
                }
            }

            if (cancelled) {
                perRun.resultsStream().enqueue(Result.newBuilder()
                        .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                                .setCancelled(com.couchbase.client.protocol.streams.Cancelled.newBuilder().setStreamId(streamId)))
                        .build());

                logger.info("Streamer {} has been cancelled after streaming back {} results", streamId, streamed.get());
            } else {
                perRun.resultsStream().enqueue(Result.newBuilder()
                        .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                                .setComplete(com.couchbase.client.protocol.streams.Complete.newBuilder().setStreamId(streamId)))
                        .build());

                logger.info("Streamer {} has finished streaming back {} results", streamId, streamed.get());
            }
        } catch (Throwable err) {
            logger.error("Streamer {} died with {}", streamId, err.toString());
            perRun.resultsStream().enqueue(Result.newBuilder()
                    .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                            .setError(com.couchbase.client.protocol.streams.Error.newBuilder()
                                    .setException(convertException.apply(err))
                                    .setStreamId(streamId)))
                    .build());
        }
    }

    public void cancel() {
        cancelled = true;
    }

    public void requestItems(RequestItemsRequest request) {
        if (demanded != 0) {
            throw new RuntimeException("More items requested on stream " + streamId + " before the previously requested items were streamed");
        }
        demanded = request.getNumItems();
    }
}

