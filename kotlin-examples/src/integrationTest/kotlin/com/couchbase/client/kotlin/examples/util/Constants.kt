package com.couchbase.client.kotlin.examples.util

import com.couchbase.client.kotlin.Keyspace

/**
 *
 * Just some constants for testing purposes
 *
 */

const val TEST_BUCKET = "travel-sample"
const val TEST_BUCKET_INDEX = "travel-sample-index"
const val TEST_ID = "airline_10"
const val TEST_CONTENT = "Airline 10"
const val NEW_CONTENT = "Airline 11"
val TEST_KEYSPACE = Keyspace(TEST_BUCKET)
val TEST_KEYSPACE_INDEX = Keyspace(TEST_BUCKET_INDEX)
const val SECONDARY_INDEX_NAME = "test-secondary-index"
