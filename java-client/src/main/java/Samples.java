import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PropertyLoader;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Document;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.builder.GetBuilder;
import com.couchbase.client.java.builder.UpsertBuilder;
import com.couchbase.client.java.env.CouchbaseEnvironment;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class Samples {




  public static void main(String... args) {
    Collection collection = new Collection() {
      @Override
      public GetBuilder get(String id) {
        return new GetBuilder(null);
      }

      @Override
      public UpsertBuilder upsert(Document document) {
        return new UpsertBuilder(null);
      }
    };


    Document fooResult = collection
      .get("foo")
      .withTimeout(Duration.ofSeconds(2))
      .execute();

    CompletableFuture<Document> barResult = collection
      .get("bar")
      .executeAsync();

    Document execute = collection
      .upsert(new Document())
      .withPersistence(PersistTo.ONE)
      .execute();


    CouchbaseEnvironment build = CouchbaseEnvironment
      .builder()
      .userAgent(() -> "foobar")
      .load(new ConnectionStringPropertyLoader("foo"))
      .build();


  }

}
