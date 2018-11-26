import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.GetResult;
import com.couchbase.client.java.MutationResult;
import com.couchbase.client.java.MutationSpec;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.options.GetOptions.getOptions;

public class Samples {

  public static void main(String... args) throws Exception {

    CouchbaseEnvironment environment = CouchbaseEnvironment
      .builder()
      .userAgent(() -> "foobar")
      .load(new ConnectionStringPropertyLoader("foo"))
      .build();


    AsyncCollection collection = new AsyncCollection(null, null, null, null);

    /**
     * Full Doc Fetch
     */
    GetResult r1 = collection.get("id").get().get();

    JsonObject fullContent = r1.content();
    JsonArray users = r1.contentAs("users", JsonArray.class);
    JsonObject firstUser = r1.content("users[0]");
    JsonArray alsoUsers = r1.path("users").contentAs(JsonArray.class);
    Entity customUser = r1.contentAs("users[0]", Entity.class, bytes -> new Entity());

    /**
     * Sub doc Fetch
     */
    GetResult r2 = collection.get(
      "id",
      getOptions().fields("firstname", "lastname").fieldExists("admin")
    ).get().get();
    JsonObject values = r1.content();


    // convenience
    CompletableFuture<MutationResult> fullInsert = collection.insert("id", new JsonObject());
    // for something like
    collection.insert("id", new MutationSpec().upsert("", new JsonObject()));

    // subdoc
    collection.insert("id", new MutationSpec().upsert("bla.blaz", true));

  }

  static class Entity {

  }

}
