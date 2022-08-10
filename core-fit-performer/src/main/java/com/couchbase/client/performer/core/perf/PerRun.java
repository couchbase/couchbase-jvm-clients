package com.couchbase.client.performer.core.perf;

import com.couchbase.client.performer.core.metrics.MetricsReporter;
import com.couchbase.client.performer.core.stream.StreamerOwner;

import javax.annotation.Nullable;

/**
 * Holds items that have the same lifetime as the main `run` RPC.
 */
public class PerRun implements AutoCloseable {
    private final String runId;
    private final WorkloadStreamingThread resultsStream;
    private final Counters counters;
    // The StreamOwner doesn't have the same lifetime as the `run` RPC, but we do need to remove all streams that
    // are related to this run.
    private final StreamerOwner streamerOwner;
    private @Nullable final MetricsReporter metricsReporter;

    public PerRun(String runId, WorkloadStreamingThread resultsStream, Counters counters, StreamerOwner streamerOwner, @Nullable MetricsReporter metricsReporter) {
        this.runId = runId;
        this.resultsStream = resultsStream;
        this.counters = counters;
        this.streamerOwner = streamerOwner;
        this.metricsReporter = metricsReporter;
    }

    public String runId() {
        return runId;
    }

    public WorkloadStreamingThread resultsStream() {
        return resultsStream;
    }

    public Counters counters() {
        return counters;
    }

    public StreamerOwner streamerOwner() {
        return streamerOwner;
    }

    @Override
    public void close() throws Exception {
        if (metricsReporter != null) {
            metricsReporter.interrupt();
            metricsReporter.join();
        }

        streamerOwner.waitForAllStreamsFromRun(runId);

        resultsStream.interrupt();
        resultsStream.join();
    }
}
