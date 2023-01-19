package com.couchbase.client.kotlin.examples.index

import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.examples.util.TEST_KEYSPACE_INDEX
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class SimpleIndexTest {

    @Test
    fun `create primary index`() {
        ConnectionUtils.withCluster {
            it.queryIndexes.createPrimaryIndex(TEST_KEYSPACE_INDEX)
            val indexes = it.queryIndexes.getAllIndexes(TEST_KEYSPACE_INDEX)
            val createdPrimaryIndex = indexes.find { it.primary && it.keyspace == TEST_KEYSPACE_INDEX }
            assertNotNull(createdPrimaryIndex)
        }
    }

}
