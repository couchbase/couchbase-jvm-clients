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
package com.couchbase.client.java.transactions.getmulti;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionOptionalGetMultiResult;
import com.couchbase.client.core.transaction.components.CoreTransactionGetMultiSpec;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JsonSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Stability.Internal
public class TransactionGetMultiUtil {
    private TransactionGetMultiUtil() {
    }

    public static List<CoreTransactionGetMultiSpec> convert(List<TransactionGetMultiSpec> specs) {
        List<CoreTransactionGetMultiSpec> coreSpecs = new ArrayList<>(specs.size());
        for (int i = 0; i < specs.size(); i++) {
            TransactionGetMultiSpec spec = specs.get(i);
            Collection collection = spec.collection();
            CollectionIdentifier collId = new CollectionIdentifier(collection.bucketName(), Optional.of(collection.scopeName()), Optional.of(collection.name()));
            coreSpecs.add(new CoreTransactionGetMultiSpec(collId, spec.id(), i));
        }
        return coreSpecs;
    }

    public static TransactionGetMultiResult convert(List<CoreTransactionOptionalGetMultiResult> result, List<TransactionGetMultiSpec> specs, JsonSerializer serializer) {
        List<TransactionGetMultiSpecResult> results = new ArrayList<>(result.size());
        for (int i = 0; i < result.size(); i++) {
            TransactionGetMultiSpec spec = specs.get(i);
            results.add(new TransactionGetMultiSpecResult(spec, result.get(i).internal, serializer, spec.transcoder()));
        }
        return new TransactionGetMultiResult(results);
    }

    public static List<CoreTransactionGetMultiSpec> convertReplica(List<TransactionGetMultiReplicasFromPreferredServerGroupSpec> specs) {
        List<CoreTransactionGetMultiSpec> coreSpecs = new ArrayList<>(specs.size());
        for (int i = 0; i < specs.size(); i++) {
            TransactionGetMultiReplicasFromPreferredServerGroupSpec spec = specs.get(i);
            Collection collection = spec.collection();
            CollectionIdentifier collId = new CollectionIdentifier(collection.bucketName(), Optional.of(collection.scopeName()), Optional.of(collection.name()));
            coreSpecs.add(new CoreTransactionGetMultiSpec(collId, spec.id(), i));
        }
        return coreSpecs;
    }

    public static TransactionGetMultiReplicasFromPreferredServerGroupResult convertReplica(List<CoreTransactionOptionalGetMultiResult> result, List<TransactionGetMultiReplicasFromPreferredServerGroupSpec> specs, JsonSerializer serializer) {
        List<TransactionGetMultiReplicasFromPreferredServerGroupSpecResult> results = new ArrayList<>(result.size());
        for (int i = 0; i < result.size(); i++) {
            TransactionGetMultiReplicasFromPreferredServerGroupSpec spec = specs.get(i);
            results.add(new TransactionGetMultiReplicasFromPreferredServerGroupSpecResult(spec, result.get(i).internal, serializer, spec.transcoder()));
        }
        return new TransactionGetMultiReplicasFromPreferredServerGroupResult(results);
    }
}
