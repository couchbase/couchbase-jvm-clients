import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.java.env.CouchbaseEnvironment;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class Samples {




  public static void main(String... args) {
    CouchbaseEnvironment build = CouchbaseEnvironment
      .builder()
      .userAgent(() -> "foobar")
      .load(new ConnectionStringPropertyLoader("foo"))
      .build();


  }

}
