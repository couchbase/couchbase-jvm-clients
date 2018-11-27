import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.Document;
import com.couchbase.client.java.kv.LookupSpec;
import com.couchbase.client.java.kv.MutationSpec;

public class Samples {

  public static void main(String... args) {

    CouchbaseEnvironment environment = CouchbaseEnvironment.builder().build();

    Cluster cluster = Cluster.connect(
      "couchbase://127.0.0.1",
      "Administrator",
      "password",
      environment
    );

    Bucket bucket = cluster.bucket("travel-sample");
    Collection collection = bucket.defaultCollection();


    Document document = collection.lookupIn("id", new LookupSpec().get("foo.bar"));

    collection.mutateIn("id", new MutationSpec().insert("foo.bar", new JsonObject()));


  }

}
