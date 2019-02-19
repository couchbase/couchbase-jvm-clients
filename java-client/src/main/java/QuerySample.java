import com.couchbase.client.core.Core;
import com.couchbase.client.core.endpoint.QueryEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.query.ReactiveQueryResult;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class QuerySample {

  public static void main(String... args) throws Exception {
    CoreEnvironment environment = CoreEnvironment.create("Administrator", "password");
    Core core = Core.create(environment);
    QueryEndpoint endpoint = new QueryEndpoint(new ServiceContext(core.context(),
      NetworkAddress.localhost(), 1234, ServiceType.QUERY, Optional.empty()),
      NetworkAddress.localhost(), 8093);

    endpoint.connect();

    Thread.sleep(1000);

    System.err.println(endpoint.state());

    byte[] query = "{\"statement\": \"select * from `travel-sample` \"}".getBytes(CharsetUtil.UTF_8);
    QueryRequest request = new QueryRequest(Duration.ofSeconds(10), core.context(),
      environment.retryStrategy(), environment.credentials(), query);
    endpoint.send(request);
    ReactiveQueryResult result = new ReactiveQueryResult(request.response());
    result.rows().doOnNext(n -> {
      System.out.println(n);
    }).subscribe();
  }

}
