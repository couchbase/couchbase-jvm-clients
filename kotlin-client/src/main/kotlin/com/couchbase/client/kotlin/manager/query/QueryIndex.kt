package com.couchbase.client.kotlin.manager.query

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.internal.toStringUtf8

public class QueryIndex(
    public val keyspace: Keyspace,
    public val name: String,
    public val primary: Boolean,
    public val state: String,
    public val type: String,
    public val indexKey: List<String>,
    public val condition: String?,
    public val partition: String?,
    public val raw: ByteArray,
) {
    public companion object {
        internal fun parse(jsonBytes: ByteArray): QueryIndex {
            val json = Mapper.decodeIntoTree(jsonBytes)
            val bucketId = json.path("bucket_id")
            val keyspace =
                if (bucketId.isMissingNode) Keyspace(
                    // Before 7.0 the server does not support collections,
                    // and puts the bucket name in the "keyspace_id" field.
                    bucket = json.path("keyspace_id").textValue()
                )
                else Keyspace(
                    bucket = bucketId.textValue(),
                    scope = json.path("scope_id").textValue(),
                    collection = json.path("keyspace_id").textValue()
                )

            return QueryIndex(
                keyspace = keyspace,
                name = json.get("name").asText(""),
                primary = json.path("is_primary").booleanValue(),
                state = json.path("state").asText(""),
                condition = json.path("condition").textValue()?.ifBlank { null },
                type = json.path("using").asText("gsi"),
                indexKey = (json.get("index_key") as? ArrayNode)?.map { it.asText() } ?: emptyList(),
                partition = json.path("partition").textValue()?.ifBlank { null },
                raw = jsonBytes,
            )
        }
    }

    override fun toString(): String {
        return "QueryIndex(keyspace=$keyspace, name='$name', primary=$primary, state='$state', type='$type', indexKey=$indexKey, condition=$condition, partition=$partition, raw=${raw.toStringUtf8()})"
    }
}
