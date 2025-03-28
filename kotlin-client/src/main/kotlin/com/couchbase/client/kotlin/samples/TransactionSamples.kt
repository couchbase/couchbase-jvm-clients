/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.transactions.TransactionDocumentSpec
import com.couchbase.client.kotlin.transactions.TransactionGetMultiMode
import com.couchbase.client.kotlin.transactions.TransactionGetResult
import kotlin.random.Random


internal suspend fun simpleTransactionExample(
    cluster: Cluster,
    collection: Collection,
    sourceDocId: String,
    destDocId: String,
    value: Int
) {
    // Assume two documents both contain integers.
    // Subtract a value from the one document and add it to the other.
    cluster.transactions.run {
        // The SDK may execute this lambda multiple times
        // if there are conflicts between transactions.
        // All logic related to the transaction must happen
        // inside this lambda.

        // Inside the lambda, `this` is a `TransactionAttemptContext`
        // with instance methods like `get`, `replace`, `insert`, `remove`,
        // and `query` for interacting with documents in a transactional way.
        // These are the only methods you should use to interact with documents
        // inside the transaction lambda.

        val source: TransactionGetResult = get(collection, sourceDocId)
        val dest: TransactionGetResult = get(collection, destDocId)

        val newSourceValue: Int = source.contentAs<Int>() - value
        val newDestValue: Int = dest.contentAs<Int>() + value

        replace(source, newSourceValue)

        // Throwing any exception triggers a rollback and causes
        // `transactions.run` to throw TransactionFailedException.
        if (Random.nextBoolean()) throw RuntimeException("simulated error")
        require(newSourceValue >= 0) { "transfer would result in negative source value" }
        require(newDestValue >= 0) { "transfer would result in dest value overflow" }

        replace(dest, newDestValue)
    }
}

internal suspend fun transactionGetMulti(
    cluster: Cluster,
    collection: Collection,
) {
    // Get multiple documents at once, making great effort to avoid read skew.
    cluster.transactions.run {
        val spec1 = TransactionDocumentSpec(collection, "someDocumentId")

        // Or use shorthand:
        val spec2 = collection + "anotherDocumentId"

        val result = getMulti(
            specs = listOf(spec1, spec2),
            mode = TransactionGetMultiMode.prioritizeReadSkewDetection(),
        )

        println(result.getOrNull(spec1)?.content)
        println(result.getOrNull(spec2)?.content)
    }
}
