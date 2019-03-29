package com.couchbase.client.java.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * Stores any non-rows results related to the execution of a particular N1QL query.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class QueryMeta {
    private final QueryResponse response;
    private final QueryChunkTrailer trailer;

    @Stability.Internal
    public QueryMeta(QueryResponse response,
                            QueryChunkTrailer trailer) {
        this.response = response;
        this.trailer = trailer;
    }

    /**
     * Returns the request identifier string of the query request
     */
    public String requestId() {
        return this.response.header().requestId();
    }

    /**
     * Returns the client context identifier string set on the query request, if it's available
     */
    public Optional<String> clientContextId() {
        return this.response.header().clientContextId();
    }

    /**
     * Returns the raw query execution status as returned by the query engine
     */
    public String queryStatus() {
        return this.trailer.status();
    }

    /**
     * Returns the signature as returned by the query engine which is then decoded to {@link JsonObject}
     * <p>
     * It is returned as an Optional which will be empty if no signature information is available.
     *
     * @throws DecodingFailedException when the signature cannot be decoded successfully
     */
    public Optional<JsonObject> signature() {
        return response.header().signature().map(v -> {
            try {
                return JacksonTransformers.MAPPER.readValue(v, JsonObject.class);
            } catch (IOException ex) {
                throw new DecodingFailedException(ex);
            }
        });
    }

    /**
     * Returns the profiling information returned by the query engine which is then decoded to {@link JsonObject}
     * <p>
     * It is returned as an Optional which will be empty if no profile information is available.
     *
     * @throws DecodingFailedException when the profile cannot be decoded successfully
     */
    public Optional<JsonObject> profileInfo() {
        return trailer.profile().map(profile -> {
            try {
                return JacksonTransformers.MAPPER.readValue(profile, JsonObject.class);
            } catch (IOException ex) {
                throw new DecodingFailedException(ex);
            }
        });
    }

    /**
     * Returns the {@link QueryMetrics} as returned by the query engine
     * <p>
     * It is returned as an Optional which will be empty if no metrics information is available.
     *
     * @throws DecodingFailedException when the metrics cannot be decoded successfully
     */
    public Optional<QueryMetrics> metrics() {
        return this.trailer.metrics().map(v -> {
            try {
                JsonObject jsonObject = JacksonTransformers.MAPPER.readValue(v, JsonObject.class);
                return new QueryMetrics(jsonObject);
            } catch (IOException ex) {
                throw new DecodingFailedException(ex);
            }
        });
    }

    /**
     * Returns any warnings returned by the query engine, as a {@link JsonArray}.
     * <p>
     * It is returned as an Optional which will be empty if no warnings were returned
     *
     * @throws DecodingFailedException when the warnings cannot be decoded successfully
     */
    public Optional<JsonArray> warnings() {
        return this.trailer.warnings().map(warnings -> {
            try {
                return JacksonTransformers.MAPPER.readValue(warnings, JsonArray.class);
            } catch (IOException ex) {
                throw new DecodingFailedException(ex);
            }
        });
    }
}
