import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.kv.GetCollectionIdRequest;
import com.couchbase.client.core.msg.kv.GetCollectionIdResponse;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import static com.couchbase.client.java.kv.GetSpec.getSpec;
import static com.couchbase.client.java.kv.MutateSpec.mutationSpec;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;

public class Samples {

  public static void main(String... args) throws Exception {
    HashSet<NetworkAddress> seeds = new HashSet<>();
    seeds.add(NetworkAddress.create("10.143.192.101"));

    ClusterEnvironment.Builder env = ClusterEnvironment.builder("Administrator", "password")
      .seedNodes(seeds);
    Cluster cluster = Cluster.connect(env.build());

    Bucket bucket = cluster.bucket("travel-sample");

    Collection airlines = bucket.collection("airlines");
    System.out.println(airlines.get("airline_10"));

    Collection dc = bucket.defaultCollection();
    System.out.println(dc.get("airline_10"));
  }

  static void scenarioA(final Collection collection) {
    Optional<GetResult> document = collection.get("id");

    if (document.isPresent()) {
      JsonObject content = document.get().contentAsObject();
      content.put("modified", true);
      MutationResult result = collection.replace("id", content);
    }
  }

  static void scenarioB(final Collection collection) {
    Optional<GetResult> document = collection.get("id", getSpec().getField("users"));

    if (document.isPresent()) {
      JsonArray content = document.get().contentAsArray();
      content.insert(0, true);
      MutationResult result = collection.mutate("id", mutationSpec().replace("users", content));
    }
  }

  static void scenarioC(final Collection collection) {
    MutationResult result = collection.remove("id", removeOptions().persistTo(PersistTo.ONE));
  }

  static void scenarioD(final Collection collection) {
    do {
      GetResult read = collection.get("id").get();
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
    Optional<GetResult> document = collection.get("id");

    if (document.isPresent()) {
      Entity content = document.get().contentAs(Entity.class);
      content.modified = true;
      MutationResult result = collection.replace("id", content);
    }
  }

  static void scenarioF(final Collection collection) {
    Optional<GetResult> document = collection.get("id");

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
