import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.Document;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.LookupSpec;
import com.couchbase.client.java.kv.MutationSpec;
import com.couchbase.client.java.kv.PersistTo;

import java.util.Optional;

import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;

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

    // full doc fetch, turns into subdoc actually
    Optional<Document> fullDoc = collection.get("id", getOptions().withExpiration(true));

    // full doc insert (a document!)
    collection.insert(
      Document.create("id", new JsonObject()), insertOptions().persistTo(PersistTo.NONE)
    );

    // subdoc fetch
    Document document = collection.lookupIn("id", new LookupSpec().get("foo.bar"));

    // subdoc mutation
    collection.mutateIn("id", new MutationSpec().insert("foo.bar", new JsonObject()));
  }

}
