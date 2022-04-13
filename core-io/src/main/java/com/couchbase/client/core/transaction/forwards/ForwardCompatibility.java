/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.forwards;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.transaction.ForwardCompatibilityFailureException;
import com.couchbase.client.core.error.transaction.internal.ForwardCompatibilityRequiresRetryException;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.util.CoreTransactionsSchedulers;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Basically, whether to retry or fast fail the transaction.
 */
@Stability.Internal
enum ForwardCompatBehaviour {
    CONTINUE,
    RETRY_TRANSACTION,
    FAST_FAIL_TRANSACTION;

    public static ForwardCompatBehaviour create(String v) {
        return v.equals("r") ? ForwardCompatBehaviour.RETRY_TRANSACTION : ForwardCompatBehaviour.FAST_FAIL_TRANSACTION;
    }
}

/**
 * Groups a simple ForwardCompatBehaviour together with some toggles to control any behaviour.  Currently, just allows
 * specifying a delay before any retries.
 */
@Stability.Internal
class ForwardCompatBehaviourFull {
    public final ForwardCompatBehaviour behaviour;
    public final Optional<Integer> retryAfterMillis;

    ForwardCompatBehaviourFull(JsonNode json) {
        Objects.requireNonNull(json);
        behaviour = ForwardCompatBehaviour.create(json.path("b").textValue());
        JsonNode raj = json.get("ra");
        if (raj != null) {
            retryAfterMillis = Optional.of(raj.asInt());
        }
        else {
            retryAfterMillis = Optional.empty();
        }
    }

    ForwardCompatBehaviourFull(ForwardCompatBehaviour behaviour, Optional<Integer> retryAfterMillis) {
        this.behaviour = Objects.requireNonNull(behaviour);
        this.retryAfterMillis = Objects.requireNonNull(retryAfterMillis);
    }

    public static final ForwardCompatBehaviourFull CONTINUE = new ForwardCompatBehaviourFull(ForwardCompatBehaviour.CONTINUE, Optional.empty());
}


/**
 * All the ForwardCompatRequirement for a given stage.
 * <p>
 * Sample input:
 * <p>
 * [{m:"X", b:"r"}, {p:"2.2", b:"f"}]
 */
@Stability.Internal
class ForwardCompatibilityForStage {
    private final List<ForwardCompatRequirement> bases = new ArrayList<>();

    ForwardCompatibilityForStage(JsonNode array) {
        Objects.requireNonNull(array);
        for (JsonNode o : array) {
            if (o.has("e") && o.has("b")) {
                bases.add(new ForwardCompatExtensionRequirement(o));
            } else if (o.has("p") && o.has("b")) {
                bases.add(new ForwardCompatProtocolRequirement(o));
            }
        }
    }

    public ForwardCompatBehaviourFull behaviour(Supported supported) {
        for (ForwardCompatRequirement b : bases) {
            ForwardCompatBehaviourFull be = b.behaviour(supported);

            if (be.behaviour != ForwardCompatBehaviour.CONTINUE) {
                return be;
            }
        }

        return ForwardCompatBehaviourFull.CONTINUE;
    }

}

/**
 * A mechanism to help ensure that older clients can be prevented from interacting with transactions created by future
 * clients that they do not know how to process.
 * <p>
 * Sample map:
 * <p>
 * fc: {
 * "WWC_OW": [{m:"X", b:"r"}, {p:"2.2", b:"f"}]
 * "CL": [{p:"2.2", b:"f"}],
 * }
 */
@Stability.Internal
public class ForwardCompatibility {
    private final Map<String, ForwardCompatibilityForStage> compatibilityMap = new HashMap<>();
    // Keep the raw JSON for debugging purposes
    private final JsonNode raw;

    private static RuntimeException RETRY = new ForwardCompatibilityRequiresRetryException();
    private static RuntimeException NO_RETRY = new ForwardCompatibilityFailureException();

    public ForwardCompatibility(JsonNode json) {
        Objects.requireNonNull(json);
        raw = json;

        Iterator<String> name = json.fieldNames();
        while (name.hasNext()) {
            String n = name.next();
            JsonNode a = json.path(n);
            compatibilityMap.put(n, new ForwardCompatibilityForStage(a));
        }
    }

    public ForwardCompatBehaviourFull check(ForwardCompatibilityStage fc, Supported supported) {
        if (compatibilityMap.containsKey(fc.value())) {
            ForwardCompatibilityForStage f = compatibilityMap.get(fc.value());
            return f.behaviour(supported);
        } else {
            return ForwardCompatBehaviourFull.CONTINUE;
        }
    }

    /**
     * Returns empty if it's ok to continue, otherwise an error:
     * <p>
     * Throws ForwardCompatibilityRequiresRetry if the 'thing' (transaction or cleanup attempt) should be retried
     * Throws ForwardCompatibilityFailure else if the 'thing' should fast-fail
     */
    public static Mono<Void> check(Core core,
                                   ForwardCompatibilityStage fc,
                                   Optional<ForwardCompatibility> forwardCompatibility,
                                   @Nullable CoreTransactionLogger logger,
                                   Supported supported) {
        return Mono.defer(() -> {
            if (forwardCompatibility.isPresent()) {
                ForwardCompatibility map = forwardCompatibility.get();

                ForwardCompatBehaviourFull behaviour = map.check(fc, supported);

                if (behaviour.behaviour == ForwardCompatBehaviour.CONTINUE) {
                    return Mono.empty();
                } else {
                    RuntimeException toThrow = (behaviour.behaviour == ForwardCompatBehaviour.RETRY_TRANSACTION) ? RETRY : NO_RETRY;

                    if (logger != null) {
                        logger.warn("", String.format("forward-compatibility rejection at point '%s'/'%s', map is %s, supported is %s",
                                fc.name(), fc.value(), map.raw, supported));
                    }

                    if (behaviour.retryAfterMillis.isPresent()) {
                        return Mono.delay(Duration.ofMillis(behaviour.retryAfterMillis.get()), core.context().environment().transactionsSchedulers().scheduler())
                                .then(Mono.error(toThrow));
                    } else {
                        return Mono.error(toThrow);
                    }
                }
            } else {
                // Must be dealing with protocol 1
                return Mono.empty();
            }
        });
    }

    @Override
    public String toString() {
        return raw.toString();
    }
}
