import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Document;
import com.couchbase.client.java.GetResult;
import com.couchbase.client.java.MutationResult;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.options.GetOptions;
import com.couchbase.client.java.options.InsertOptions;
import com.couchbase.client.java.options.UpsertOptions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.couchbase.client.java.options.GetOptions.getOptions;
import static com.couchbase.client.java.options.InsertOptions.insertOptions;

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
    GetResult r1 = collection.get("id").get();

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
    ).get();
    JsonObject values = r1.content();





    CompletableFuture<MutationResult> result = collection.insert("id", new JsonObject());


    collection.insert("id", new JsonObject());

  }

  static class Entity {

  }

}
