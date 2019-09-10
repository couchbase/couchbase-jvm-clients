package com.couchbase.client.java.examples.query;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.examples.kv.Person;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;

import java.time.Duration;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class EntityQueryExample {

  public static void main(String... args) {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("travel-sample");

    String statement = "SELECT `travel-sample`.* FROM `travel-sample` WHERE type=$type";
    QueryResult result = cluster.query(
      statement,
      queryOptions()
        .timeout(Duration.ofSeconds(75))
        .parameters(JsonObject.create().put("type", "person"))
    );

    for (Person person : result.rowsAs(Person.class)) {
      System.out.println(person.name());
    }


  }
}
