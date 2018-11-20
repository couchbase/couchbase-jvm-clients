import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Document;
import com.couchbase.client.java.MutationResult;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.options.GetOptions;
import com.couchbase.client.java.options.UpsertOptions;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Samples {

  public static void main(String... args) {

    CouchbaseEnvironment environment = CouchbaseEnvironment
      .builder()
      .userAgent(() -> "foobar")
      .load(new ConnectionStringPropertyLoader("foo"))
      .build();


    Collection collection = new Collection(null, null, null, null);

    Document<JsonObject> id = collection.get("id");



  }

}
