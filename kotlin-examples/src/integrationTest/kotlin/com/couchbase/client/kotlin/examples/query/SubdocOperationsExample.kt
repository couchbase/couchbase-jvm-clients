package com.couchbase.client.kotlin.examples.query

import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.MutateInSpec
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

private const val BUCKET_NAME = "travel-sample-subdoc"

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class SubdocOperationsExample {

    companion object {
        @JvmStatic
        @BeforeAll
        fun setup() {
	        ConnectionUtils.withCluster { cluster ->
		        cluster.buckets.createBucket(BUCKET_NAME)
		        cluster.queryIndexes.createPrimaryIndex(Keyspace(BUCKET_NAME))
		        val data = mapOf(
			        "name" to "Product 1",
			        "price" to 9.99,
			        "orders" to listOf(1, 2, 3)
		        )
		        val bucket = cluster.bucket(BUCKET_NAME)
		        bucket.defaultCollection().insert("product_1", data)
	        }
        }
    }

    @Test
    @Order(1)
    fun `retrieve a document with lookup in spec and lambda`() {
	    ConnectionUtils.withCluster { cluster ->
		    val spec = object : LookupInSpec() {
			    val name = get("name")
			    val price = exists("price")
			    val amount = exists("amount")
			    val orders = count("orders")
		    }
		    val bucket = cluster.bucket(BUCKET_NAME)
            bucket.defaultCollection().lookupIn("product_1", spec) {
			    with(spec) {
				    assertEquals("Product 1", name.contentAs<String>())
				    assertEquals(3, orders.value)
				    assertEquals(true, price.value)
				    assertEquals(false, amount.value)
			    }
		    }

	    }
    }

    @Test
    @Order(2)
    fun `update a document with mutate in spec`() {
        ConnectionUtils.withCluster { cluster ->
            val spec = MutateInSpec()
            spec.replace("name", "Product 1 (updated)")
            spec.insert("amount", 10)
            spec.remove("price")
            spec.arrayAddUnique("orders", 4L)
            val bucket = cluster.bucket(BUCKET_NAME)
            bucket.defaultCollection().mutateIn("product_1", spec)
            val result = bucket.defaultCollection().get("product_1").contentAs<Map<String, Any>>()
            assertEquals("Product 1 (updated)", result["name"])
            assertEquals(10, result["amount"])
            assertEquals(listOf(1, 2, 3, 4), result["orders"])
            assertTrue(!result.containsKey("price"))
        }
    }

}
