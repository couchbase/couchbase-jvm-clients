/*
 * Copyright 2021 Couchbase, Inc.
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

import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.kv.LookupInSpec

internal suspend fun subdocLookup(collection: Collection, documentId: String) {
    // Subdoc lookup
    val spec = object : LookupInSpec() {
        val sku = get("sku")
        val hasPrice = exists("price")
        val orderCount = count("orders")
    }
    collection.lookupIn(documentId, spec) {
        println("cas: $cas")
        with(spec) {
            println("sku: ${sku.contentAs<String>()}")
            println("has price: ${hasPrice.value}")
            println("order count: ${orderCount.value}")
        }
    }
}

internal suspend fun subdocLookupWithoutLambda(collection: Collection, documentId: String) {
    // Subdoc lookup without Lambda
    val spec = object : LookupInSpec() {
        val sku = get("sku")
        val hasPrice = exists("price")
        val orderCount = count("orders")
    }
    val result = collection.lookupIn(documentId, spec)
    println("cas: $result.cas")
    with(spec) {
        println("sku: ${sku.contentAs<String>(result)}")
        println("has price: ${hasPrice.get(result)}")
        println("order count: ${orderCount.get(result)}")
    }
}
