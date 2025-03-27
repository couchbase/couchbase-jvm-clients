/*
 * Copyright 2025 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.getmulti;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionGetMultiResult;
import com.couchbase.client.core.transaction.CoreTransactionOptionalGetMultiResult;
import com.couchbase.client.core.transaction.components.CoreTransactionGetMultiSpec;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.transaction.getmulti.CoreGetMultiSignal.RESET_AND_RETRY;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreGetMultiState {
    public static final Duration DEFAULT_INITIAL_DOC_FETCH_BOUND = Duration.ofMillis(2500);
    public static final Duration DEFAULT_READ_SKEW_BOUND = Duration.ofMillis(100);

    public static final List<CoreTransactionGetMultiSpec> EMPTY_GET_MULTI_SPEC_LIST = new ArrayList<>();
    public static final List<CoreTransactionOptionalGetMultiResult> EMPTY_GET_MULTI_RESULT_LIST = new ArrayList<>();

    public final List<CoreTransactionGetMultiSpec> originalSpecs;
    public final CoreGetMultiOptions options;

    private List<CoreTransactionGetMultiSpec> toFetch;
    private List<CoreTransactionOptionalGetMultiResult> alreadyFetched = EMPTY_GET_MULTI_RESULT_LIST;
    private CoreGetMultiPhase phase = CoreGetMultiPhase.FIRST_DOC_FETCH;
    public Instant deadline;
    public final boolean replicasFromPreferredServerGroup;

    @Override
    public String toString() {
        return "GetMultiState{" +
                "originalSpecs=" + originalSpecs.size() +
                ", toFetch=" + toFetch.size() +
                ", alreadyFetched=" + alreadyFetched.size() +
                ", mode=" + phase +
                ", deadline=" + deadline +
                ", mode=" + options.mode +
                ", replicasFromPreferredServerGroup=" + replicasFromPreferredServerGroup +
                '}';
    }

    public CoreGetMultiState(List<CoreTransactionGetMultiSpec> toFetch,
                             Instant deadline,
                             boolean replicasFromPreferredServerGroup,
                             CoreGetMultiOptions options) {
        // Defensive copy to prevent the user changing these under us
        this.originalSpecs = new ArrayList<>(requireNonNull(toFetch));
        this.toFetch = originalSpecs;
        this.deadline = requireNonNull(deadline);
        this.replicasFromPreferredServerGroup = replicasFromPreferredServerGroup;
        this.options = requireNonNull(options);
    }

    public boolean deadlineExceededSoon() {
        return Instant.now().isAfter(deadline.minusMillis(5));
    }

    public List<CoreTransactionGetMultiSpec> toFetch() {
        return toFetch;
    }

    public CoreGetMultiSignalAndReason update(CoreTransactionLogger logger,
                                              List<CoreTransactionGetMultiSpec> toFetch,
                                              List<CoreTransactionOptionalGetMultiResult> alreadyFetched,
                                              CoreGetMultiPhase mode,
                                              Instant deadline) {
        this.toFetch = toFetch;
        this.alreadyFetched = alreadyFetched;
        this.phase = mode;
        this.deadline = deadline;
        logger.info("<>", "Updated state to {}", this);
        return assertValidState();
    }

    public List<CoreTransactionOptionalGetMultiResult> alreadyFetched() {
        return alreadyFetched;
    }

    public CoreGetMultiPhase phase() {
        return phase;
    }

    public List<CoreTransactionGetMultiResult> fetchedAndPresent() {
        return alreadyFetched.stream()
                .filter(v -> v.internal.isPresent())
                .map(CoreTransactionOptionalGetMultiResult::get)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent") // safe due to isDocumentInTransaction call
    public CoreGetMultiSignalAndReason assertInReadSkewResolutionState() {
        // 2+ docs and 1+ of them are staged in the same transaction T1.
        List<CoreTransactionGetMultiResult> fetchedAndPresent = fetchedAndPresent();

        Set<String> transactionIdsInvolved = fetchedAndPresent.stream()
                .filter(v -> requireNonNull(v.internal.links()).isDocumentInTransaction())
                .map(v -> requireNonNull(v.internal.links()).stagedTransactionId().get())
                .collect(Collectors.toSet());

        if (fetchedAndPresent.size() < 2) {
            return new CoreGetMultiSignalAndReason(RESET_AND_RETRY, "getMulti internal bug detected - have less than 2 in " + fetchedAndPresent);
        }

        if (transactionIdsInvolved.size() != 1) {
            return new CoreGetMultiSignalAndReason(RESET_AND_RETRY, "getMulti internal bug detected - have more than 1 T1 unexpected " + transactionIdsInvolved);
        }

        return CoreGetMultiSignalAndReason.CONTINUE;
    }

    public CoreGetMultiSignalAndReason assertValidState() {
        if (alreadyFetched.size() + toFetch.size() != originalSpecs.size()) {
            return new CoreGetMultiSignalAndReason(RESET_AND_RETRY, "getMulti internal bug detected - alreadyFetched+toFetch!=originalSpecs " + this);
        }

        return CoreGetMultiSignalAndReason.CONTINUE;
    }

    public void reset(CoreTransactionLogger logger) {
        update(logger, originalSpecs, EMPTY_GET_MULTI_RESULT_LIST,
                phase == CoreGetMultiPhase.FIRST_DOC_FETCH
                    ? CoreGetMultiPhase.FIRST_DOC_FETCH
                    : CoreGetMultiPhase.SUBSEQUENT_TO_FIRST_DOC_FETCH,
                deadline);
    }
}
