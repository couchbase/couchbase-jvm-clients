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
public class StreamStreamer<T> extends IteratorBasedStreamer {
    private final Iterator<T> iterator;

    public StreamStreamer(Stream<T> results, PerRun perRun, String streamId, Config streamConfig, Function<T, Result> convert) {
        super(perRun, streamId, streamConfig, convert);
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
