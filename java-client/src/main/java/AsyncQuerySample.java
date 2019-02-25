import com.couchbase.client.core.Core;
import com.couchbase.client.core.endpoint.QueryEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.SimpleQuery;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class AsyncQuerySample {

	public static void main(String... args) throws Exception {
		CoreEnvironment environment = CoreEnvironment.builder("Administrator", "password").build();
		Core core = Core.create(environment);
		QueryEndpoint endpoint = new QueryEndpoint(new ServiceContext(core.context(),
						NetworkAddress.localhost(), 1234, ServiceType.QUERY, Optional.empty()),
						NetworkAddress.localhost(), 8093);

		endpoint.connect();

		Thread.sleep(1000);

		System.err.println(endpoint.state());


		Query query = SimpleQuery.create("select * from `travel-sample` limit 32000");
		QueryRequest request = new QueryRequest(Duration.ofSeconds(10), core.context(),
						environment.retryStrategy(), environment.credentials(), query.encode());

		endpoint.send(request);
		CompletableFuture<AsyncQueryResult> result = request.response().thenApply(AsyncQueryResult::new);
		List<JsonObject> rows = result.thenApply(AsyncQueryResult::rows).get().get();
		System.out.println(rows.size());
	}
}
