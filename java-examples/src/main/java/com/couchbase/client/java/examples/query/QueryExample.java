package com.couchbase.client.java.examples.query;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.SimpleQuery;

public class QueryExample {

  public static void main(String... args) {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    cluster.bucket("travel-sample");
    QueryResult result = cluster.query("select * from `travel-sample` limit 1");
    System.out.println(result.signature());
  }

}
