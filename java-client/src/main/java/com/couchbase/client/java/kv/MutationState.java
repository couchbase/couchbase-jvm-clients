/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.java.kv;

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.MutationTokenAggregator;
import com.couchbase.client.java.json.JsonObject;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * Aggregation of one or more {@link MutationToken}s for specifying
 * consistency requirements of N1QL or FTS queries.
 * <p>
 * Thread-safe.
 *
 * @since 2.3.0
 */
public class MutationState implements Iterable<MutationToken> {

    private final MutationTokenAggregator tokens;

    private MutationState() {
        this.tokens = new MutationTokenAggregator();
    }

    private MutationState(MutationTokenAggregator tokens) {
        this.tokens = requireNonNull(tokens);
    }

    /**
     * Create a {@link MutationState} from one or more {@link MutationToken MutationTokens}.
     *
     * @param mutationTokens the mutation tokens.
     * @return the initialized {@link MutationState}.
     */
    public static MutationState from(final MutationToken... mutationTokens) {
        return new MutationState().add(mutationTokens);
    }

    /**
     * Add one or more {@link MutationToken MutationTokens} to this {@link MutationState}.
     *
     * @param mutationTokens the tokens
     * @return the modified {@link MutationState}.
     */
    public MutationState add(final MutationToken... mutationTokens) {
        if (mutationTokens == null || mutationTokens.length == 0) {
            throw InvalidArgumentException.fromMessage("At least one MutationToken must be provided.");
        }
        for (MutationToken t : mutationTokens) {
            tokens.add(t);
        }
        return this;
    }

    /**
     * Adds all the internal state from the given {@link MutationState} onto the called one.
     *
     * @param mutationState the state from which the tokens are applied from.
     * @return the modified {@link MutationState}.
     */
    public MutationState add(final MutationState mutationState) {
        for (MutationToken token : mutationState) {
            tokens.add(token);
        }
        return this;
    }

    @Override
    public Iterator<MutationToken> iterator() {
        return tokens.iterator();
    }

    /**
     * Exports the {@link MutationState} into a universal format, which can be used either to serialize it into
     * a N1QL query or to send it over the network to a different application/SDK.
     *
     * @return the exported {@link JsonObject}.
     */
    public JsonObject export() {
        return JsonObject.from(tokens.export());
    }

    /**
     * Exports the {@link MutationState} into a format recognized by the FTS search engine.
     *
     * @return the exported {@link JsonObject} for one FTS index.
     */
    public JsonObject exportForSearch() {
        return JsonObject.from(tokens.exportForSearch());
    }

    /**
     * Create a {@link MutationState} from the serialized state.
     *
     * @param source the source state, serialized.
     * @return the created {@link MutationState}.
     */
    public static MutationState from(String source) {
        return from(JsonObject.fromJson(source));
    }

    /**
     * Create a {@link MutationState} from the serialized state.
     *
     * @param source the source state, serialized.
     * @return the created {@link MutationState}.
     */
    public static MutationState from(JsonObject source) {
        return new MutationState(MutationTokenAggregator.from(source.toMap()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MutationState state = (MutationState) o;
        return tokens.equals(state.tokens);
    }

    @Override
    public int hashCode() {
        return tokens.hashCode();
    }

    @Override
    public String toString() {
        return "MutationState{tokens=" + tokens + '}';
    }
}
