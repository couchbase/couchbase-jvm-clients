import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.Document;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationSpec;
import com.couchbase.client.java.kv.PersistTo;

import java.util.Optional;

import static com.couchbase.client.java.kv.LookupSpec.lookupSpec;
import static com.couchbase.client.java.kv.MutationSpec.mutationSpec;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;

public class Samples {

  public static void main(String... args) {

  }

  static Collection connect() {
    CouchbaseEnvironment environment = CouchbaseEnvironment.builder().build();

    Cluster cluster = Cluster.connect(
      "couchbase://127.0.0.1",
      "Administrator",
      "password",
      environment
    );

    Bucket bucket = cluster.bucket("travel-sample");
    return bucket.defaultCollection();
  }

  // TODO: with lazy decode, need to create a new document here??
  // .. people might then store the original object and be confused why it hasn't changed
  static void scenarioA(final Collection collection) {
    Optional<Document> document = collection.get("id");

    if (document.isPresent()) {
      JsonObject content = document.get().contentAsObject();
      content.put("modified", true);
      MutationResult result = collection.insert(
        Document.create("id", content)
      );
    }
  }

  // TODO: lookupIn also needs to return an optional, since the outer doc might not be found
  static void scenarioB(final Collection collection) {
    Optional<Document> document = collection.lookupIn("id", lookupSpec().get("users"));

    if (document.isPresent()) {
      JsonArray content = document.get().contentAsArray();
      content.insert(0, true);
      MutationResult result = collection.mutateIn(
        "id",
        mutationSpec().replace("users", content)
      );
    }
  }

  static void scenarioC(final Collection collection) {
    MutationResult result = collection.remove("id", removeOptions().persistTo(PersistTo.ONE));
  }

  // todo: cas.get() kinda redundant since we know it can be returned from a get?
  static void scenarioD(final Collection collection) {
    do {
      Document document = collection.get("id").get();
      JsonObject content = document.contentAsObject();
      content.put("modified", true);
      try {
        MutationResult result = collection.insert(
          Document.create("id", content).withCas(document.cas().get())
        );
      } catch (CasMismatchException ex) {
        continue;
      }
    } while(false);
  }

  static void scenarioE(final Collection collection) {
    Optional<Document> document = collection.get("id");

    if (document.isPresent()) {
      Entity content = document.get().contentAs(Entity.class);
      content.modified = true;
      MutationResult result = collection.insert(
        Document.create("id", content)
      );
    }
  }

  static void scenarioF(final Collection collection) {
    Optional<Document> document = collection.get("id");

    if (document.isPresent()) {
      Entity content = document.get().contentAs(Entity.class);
      content.modified = true;
      MutationResult result = collection.insert(
        Document.create("id", content)
      );
    }
  }

  class Entity {
    boolean modified;
  }

}
