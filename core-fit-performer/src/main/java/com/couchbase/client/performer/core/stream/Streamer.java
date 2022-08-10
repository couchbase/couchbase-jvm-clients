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
    protected final Function<T, Result> convert;
    protected final AtomicInteger streamed = new AtomicInteger(0);

    public Streamer(PerRun perRun,
                    String streamId,
                    Config streamConfig,
                    Function<T, Result> convert) {
        this.perRun = perRun;
        this.streamId = streamId;
        this.streamConfig = streamConfig;
        this.convert = convert;
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
