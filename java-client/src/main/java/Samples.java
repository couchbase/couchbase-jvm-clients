import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.java.env.CouchbaseEnvironment;

public class Samples {

  public static void main(String... args) {

    CouchbaseEnvironment environment = CouchbaseEnvironment
      .builder()
      .userAgent(() -> "foobar")
      .load(new ConnectionStringPropertyLoader("foo"))
      .build();

  }

}
