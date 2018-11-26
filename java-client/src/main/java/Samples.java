import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;

import java.util.Optional;

import static com.couchbase.client.java.kv.FullInsertOptions.insertOptions;


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
    Collection collection = bucket.collection("collection", "scope");

    Optional<GetResult> getResult = collection.get("id");

    MutationResult mutationResult = collection.insert(
      "id",
      new JsonObject(),
      insertOptions().persistTo(PersistTo.NONE)
    );

  }


}
