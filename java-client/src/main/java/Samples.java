import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Document;
import com.couchbase.client.java.MutationResult;
import com.couchbase.client.java.env.CouchbaseEnvironment;
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

  public static void main(String... args) {

    CouchbaseEnvironment environment = CouchbaseEnvironment
      .builder()
      .userAgent(() -> "foobar")
      .load(new ConnectionStringPropertyLoader("foo"))
      .build();


    AsyncCollection collection = new AsyncCollection(null, null, null, null);


    CompletableFuture<Document<JsonObject>> res = collection.get("id", getOptions());
    CompletableFuture<Document<JsonObject>> res4 = collection.get(
      "id",
      getOptions().decoder(bytes -> null)
    );

    CompletableFuture<Document<Integer>> res2 = collection.get("id", getOptions(Integer.class));
    CompletableFuture<Document<Integer>> res3 = collection.get(
      "id",
      getOptions(Integer.class).decoder(bytes -> null)
    );

    CompletableFuture<MutationResult> result = collection.insert("id", new JsonObject());


    collection.insert("id", new JsonObject());

  }

}
