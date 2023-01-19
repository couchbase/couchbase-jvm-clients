package com.couchbase.client.kotlin.examples.index

import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.examples.util.SECONDARY_INDEX_NAME
import com.couchbase.client.kotlin.examples.util.TEST_KEYSPACE_INDEX
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class SimpleIndexTest {

    @Test
    @Order(1)
    fun `create primary index`() {
        ConnectionUtils.withCluster {
            it.queryIndexes.createPrimaryIndex(TEST_KEYSPACE_INDEX)
            val indexes = it.queryIndexes.getAllIndexes(TEST_KEYSPACE_INDEX)
            val createdPrimaryIndex = indexes.find { index -> index.primary }
            assertNotNull(createdPrimaryIndex)
        }
    }

    @Test
    @Order(2)
    fun `drop primary index`() {
        ConnectionUtils.withCluster {
            it.queryIndexes.dropPrimaryIndex(TEST_KEYSPACE_INDEX)
            val indexes = it.queryIndexes.getAllIndexes(TEST_KEYSPACE_INDEX)
            val primaryIndexExists = indexes.any { index -> index.primary }
            assertFalse(primaryIndexExists)
        }
    }

    @Test
    @Order(3)
    fun `create secondary index`() {
        ConnectionUtils.withCluster {
            it.queryIndexes.createIndex(TEST_KEYSPACE_INDEX, SECONDARY_INDEX_NAME, listOf("field1"))
            val indexes = it.queryIndexes.getAllIndexes(TEST_KEYSPACE_INDEX)
            val createdIndex = indexes.find { index -> index.name == SECONDARY_INDEX_NAME }
            println("!!!Index")
            println(createdIndex)
            assertNotNull(createdIndex)
        }
    }

}
