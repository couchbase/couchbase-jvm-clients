import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ReadResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;

import java.util.Optional;

import static com.couchbase.client.java.kv.MutationSpec.mutationSpec;
import static com.couchbase.client.java.kv.ReadOptions.readOptions;
import static com.couchbase.client.java.kv.ReadSpec.readSpec;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;

public class Samples {

  public static void main(String... args) {

  }

  static Collection connect() {
    CouchbaseEnvironment environment = CouchbaseEnvironment.create();

    Cluster cluster = Cluster.connect(
      "couchbase://127.0.0.1",
      "Administrator",
      "password",
      environment
    );

    Bucket bucket = cluster.bucket("travel-sample");
    return bucket.defaultCollection();
  }

  static void scenarioA(final Collection collection) {
    Optional<ReadResult> document = collection.read("id");

    if (document.isPresent()) {
      JsonObject content = document.get().contentAsObject();
      content.put("modified", true);
      MutationResult result = collection.replace("id", content);
    }
  }

  static void scenarioB(final Collection collection) {
    Optional<ReadResult> document = collection.read("id", readSpec().getField("users"));

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

  static void scenarioD(final Collection collection) {
    do {
      ReadResult read = collection.read("id").get();
      JsonObject content = read.contentAsObject();
      content.put("modified", true);
      try {
        MutationResult result = collection.replace("id", content, replaceOptions().cas(read.cas()));
      } catch (CASMismatchException ex) {
        continue;
      }
    } while(false);
  }

  static void scenarioE(final Collection collection) {
    Optional<ReadResult> document = collection.read("id");

    if (document.isPresent()) {
      Entity content = document.get().contentAs(Entity.class);
      content.modified = true;
      MutationResult result = collection.replace("id", content);
    }
  }

  static void scenarioF(final Collection collection) {
    Optional<ReadResult> document = collection.read("id");

    if (document.isPresent()) {
      Entity content = document.get().contentAs(Entity.class);
      content.modified = true;
      MutationResult result = collection.replace("id", content);
    }
  }

  class Entity {
    boolean modified;
  }

}
