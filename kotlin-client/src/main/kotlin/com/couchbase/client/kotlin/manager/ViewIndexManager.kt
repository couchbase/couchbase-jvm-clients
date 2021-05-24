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

package com.couchbase.client.kotlin.manager

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.error.DesignDocumentNotFoundException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.manager.CoreViewIndexManager
import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.view.DesignDocumentNamespace
import com.couchbase.client.kotlin.view.DesignDocumentNamespace.PRODUCTION
import kotlinx.coroutines.future.await

@OptIn(VolatileCouchbaseApi::class)
public class ViewIndexManager internal constructor(internal val bucket: Bucket) {
    private val coreManager = CoreViewIndexManager(bucket.core, bucket.name)

    public suspend fun getAllDesignDocuments(
        namespace: DesignDocumentNamespace,
        common: CommonOptions = CommonOptions.Default,
    ): List<DesignDocument> {
        val ddocNameToJson = coreManager.getAllDesignDocuments(
            namespace == PRODUCTION,
            common.toCore(),
        ).await()
        return parseAllDesignDocuments(ddocNameToJson)
    }

    /**
     * Returns the named design document from the specified namespace.
     *
     * @param name name of the design document to retrieve
     * @param namespace namespace to look in
     * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
     */
    public suspend fun getDesignDocument(
        name: String,
        namespace: DesignDocumentNamespace,
        common: CommonOptions = CommonOptions.Default,
    ): DesignDocument {
        val responseBytes = coreManager.getDesignDocument(
            name, namespace == PRODUCTION,
            common.toCore(),
        ).await()
        return parseDesignDocument(name, Mapper.decodeIntoTree(responseBytes) as ObjectNode)
    }

    public suspend fun upsertDesignDocument(
        doc: DesignDocument,
        namespace: DesignDocumentNamespace,
        common: CommonOptions = CommonOptions.Default,
    ): Unit {
        coreManager.upsertDesignDocument(
            doc.name, toJson(doc), namespace == PRODUCTION,
            common.toCore(),
        ).await()
    }

    public suspend fun publishDesignDocument(
        name: String,
        common: CommonOptions = CommonOptions.Default,
    ): Unit {
        coreManager.publishDesignDocument(
            name,
            common.toCore(),
        ).await()
    }

    public suspend fun dropDesignDocument(
        name: String,
        namespace: DesignDocumentNamespace,
        common: CommonOptions = CommonOptions.Default,
    ): Unit {
        coreManager.dropDesignDocument(
            name, namespace == PRODUCTION,
            common.toCore(),
        ).await()
    }

    private fun toJson(doc: DesignDocument): ByteArray {
        val root = Mapper.createObjectNode()
        val views = root.putObject("views")
        doc.views.forEach { (viewName, view) ->
            val viewNode = Mapper.createObjectNode()
            viewNode.put("map", view.map)
            view.reduce?.let { viewNode.put("reduce", it) }
            views.set<JsonNode>(viewName, viewNode)
        }
        return Mapper.encodeAsBytes(root)
    }

    private fun parseAllDesignDocuments(ddocNameToJson: Map<String, ObjectNode>): List<DesignDocument> {
        val result = ArrayList<DesignDocument>()
        ddocNameToJson.forEach { (ddocName, json) -> result.add(parseDesignDocument(ddocName, json)) }
        return result;
    }

    private fun parseDesignDocument(name: String, node: ObjectNode): DesignDocument {
        val views = mutableMapOf<String, View>()

        node.path("views").fields().forEach { entry ->
            val viewName = entry.key
            val map = requireNotNull(
                entry.value.path("map").textValue(),
                { "view JSON is missing expected field 'map': ${entry.value}" }
            )
            val reduce = entry.value.path("reduce").textValue()
            views[viewName] = View(map, reduce)
        }

        return DesignDocument(name.removePrefix("dev_"), views)
    }
}
