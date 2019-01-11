import com.couchbase.client.core.Core;
import com.couchbase.client.core.endpoint.QueryEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class QuerySample {

  public static void main(String... args) throws Exception {
    CoreEnvironment environment = CoreEnvironment.create("Administrator", "password");
    Core core = Core.create(environment);
    QueryEndpoint endpoint = new QueryEndpoint(core.context(), NetworkAddress.localhost(), 8093);

    endpoint.connect();

    Thread.sleep(1000);

    System.err.println(endpoint.state());

    byte[] query = "{\"statement\": \"select * from `travel-sample` limit 1000\"}".getBytes(CharsetUtil.UTF_8);
    QueryRequest request = new QueryRequest(Duration.ofSeconds(10), core.context(),
      environment.retryStrategy(), environment.credentials(), query,
      new QueryResponse.QueryEventSubscriber() {
        @Override
        public void onNext(QueryResponse.QueryEvent row) {
          System.err.println("row");
        }

        @Override
        public void onComplete() {
          System.err.println("done!");
        }
      });

    endpoint.send(request);
    QueryResponse response = request.response().get(10, TimeUnit.SECONDS);

    while(true) {
      Thread.sleep(5000);
      response.request(1);
    }
  }

}
