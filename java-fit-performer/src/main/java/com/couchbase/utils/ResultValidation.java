/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// [skip:<3.3.0]
package com.couchbase.utils;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.transactions.TransactionQueryResult;
import com.couchbase.client.protocol.transactions.CommandQuery;
import com.couchbase.client.protocol.transactions.ExpectedResult;
import com.couchbase.twoway.TestFailure;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

public class ResultValidation {
    private final static ExpectedResult ANYTHING_ALLOWED = ExpectedResult.newBuilder().setAnythingAllowed(true).build();

    private ResultValidation() {}

    static public void validateQueryResult(CommandQuery request, TransactionQueryResult qr) {
        validateQueryResult(request, qr.rowsAsObject(), qr.metaData().metrics().get().mutationCount());
    }

    static public void validateQueryResult(CommandQuery request, QueryResult qr) {
        validateQueryResult(request, qr.rowsAsObject(), qr.metaData().metrics().get().mutationCount());
    }

    static public void validateQueryResult(CommandQuery request, List<JsonObject> rows, long mutationCount) {
        if (request.getCheckRowCount()) {
            if (rows.size() != request.getExpectedRowCount()) {
                throw new TestFailure(new IllegalStateException(
                        String.format("Expected %d rows but only got %d", request.getExpectedRowCount(), rows.size())));
            }
        }

        if (request.getCheckMutations()) {
            if (mutationCount != request.getExpectedMutations()) {
                throw new TestFailure(new IllegalStateException(
                        String.format("Expected %d mutations but only got %d", request.getExpectedMutations(), mutationCount)));
            }
        }

        if (request.getCheckRowContent()) {
            if (rows.size() != request.getExpectedRowCount()) {
                throw new TestFailure(new IllegalStateException(
                        String.format("Expected %d rows but only got %d", request.getExpectedRowCount(), rows.size())));
            }

            for (int i = 0; i < rows.size(); i++) {
                JsonObject expected = JsonObject.fromJson(request.getExpectedRowsList().get(i));
                JsonObject row = rows.get(i);

                if (expected.size() != row.size()) {
                    throw new TestFailure(new IllegalStateException(
                            String.format("Expected %s fields in row %d but only got %d", expected.size(), i, row.size())));
                }

                for (String name : expected.getNames()) {
                    if (!row.containsKey(name)) {
                        throw new TestFailure(new IllegalStateException(
                                String.format("Expected field %s in row %d but not found", name, i)));
                    }

                    if (!row.get(name).equals(expected.get(name))) {
                        throw new TestFailure(new IllegalStateException(
                                String.format("Expected %s=%s in row %d but got %s", name, expected.get(name), i, row.get(name))));
                    }
                }
            }
        }
    }

    static public Mono<Void> validateQueryResult(CommandQuery request, ReactiveQueryResult qr) {
        return qr.rowsAsObject()
                .collectList()
                .flatMap(rows -> qr.metaData()
                        .doOnNext(metaData -> validateQueryResult(request, rows, metaData.metrics().get().mutationCount())))
                        .then();
    }

    public static boolean anythingAllowed(List<ExpectedResult> expectedResults) {
        return expectedResults.isEmpty() || expectedResults.contains(ANYTHING_ALLOWED);
    }

    public static String dbg(List<ExpectedResult> expectedResults) {
        return "one of (" + expectedResults.stream().map(v -> v.toString()).collect(Collectors.joining(",")) + ")";
    }


}
