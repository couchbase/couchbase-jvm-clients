package com.couchbase.client.kotlin.examples

import com.couchbase.client.kotlin.Cluster

object HelloWorld {

    @JvmStatic
    fun main(args: Array<String>) {
        val cluster: Cluster = Cluster.connect("couchbase://127.0.0.1", "Administrator", "password")
        println(cluster)
    }

}
