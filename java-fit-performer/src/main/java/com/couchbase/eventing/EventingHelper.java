package com.couchbase.eventing;

// [skip:<3.2.1]

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunction;
import com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionKeyspace;
import com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.GetFunctionOptions;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.utils.OptionsUtil.convertDuration;

public class EventingHelper {

  private EventingHelper() {

  }

  public static void handleEventingFunctionManager(Cluster cluster,
                                                   ConcurrentHashMap<String, RequestSpan> spans,
                                                   com.couchbase.client.protocol.sdk.Command command,
                                                   Result.Builder result) {

    var efm = command.getClusterCommand().getEventingFunctionManager();

    if (efm.hasGetFunction()) {
      var request = efm.getGetFunction();
      var name = request.getName();
      com.couchbase.client.java.manager.eventing.EventingFunction response;

      if (!request.hasOptions()) {
        response = cluster.eventingFunctions().getFunction(name);
      } else {
        var options = createGetFunctionOptions(request.getOptions(), spans);
        response = cluster.eventingFunctions().getFunction(name, options);
      }

      minimalEventingFunctionFromResult(result, response);
    }
  }


  public static Mono<Result> handleEventingFunctionManagerReactive(ReactiveCluster cluster,
                                                                   ConcurrentHashMap<String, RequestSpan> spans,
                                                                   com.couchbase.client.protocol.sdk.Command command,
                                                                   Result.Builder result) {

    var efm = command.getClusterCommand().getEventingFunctionManager();

    if (efm.hasGetFunction()) {
      Mono<com.couchbase.client.java.manager.eventing.EventingFunction> response;

      var request = efm.getGetFunction();
      var name = request.getName();

      if (!request.hasOptions()) {
        response = cluster.eventingFunctions().getFunction(name);
      } else {
        var options = createGetFunctionOptions(request.getOptions(), spans);
        response = cluster.eventingFunctions().getFunction(name, options);
      }


      return response.map(r -> {
        minimalEventingFunctionFromResult(result, r);
        return result.build();
      });

    } else {
      return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
    }
  }

  public static void minimalEventingFunctionFromResult(Result.Builder result, com.couchbase.client.java.manager.eventing.EventingFunction response) {
    var builder = EventingFunction.newBuilder();

    builder.setName(response.name())
            .setCode(response.code())
            .setMetadataKeyspace(convertKeyspace(response.metadataKeyspace()))
            .setSourceKeyspace(convertKeyspace(response.sourceKeyspace()));


    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setEventingFunctionManagerResult(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder()
                    .setEventingFunction(builder)));

  }

  public static EventingFunctionKeyspace convertKeyspace(com.couchbase.client.java.manager.eventing.EventingFunctionKeyspace oldKeyspace) {
    var newKeyspace = EventingFunctionKeyspace.newBuilder();
    newKeyspace.setBucket(oldKeyspace.bucket()).setScope(oldKeyspace.scope()).setCollection(oldKeyspace.collection());
    return newKeyspace.build();
  }

  private static com.couchbase.client.java.manager.eventing.GetFunctionOptions createGetFunctionOptions(GetFunctionOptions getFunctionOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.eventing.GetFunctionOptions.getFunctionOptions();

    if (getFunctionOptions.hasTimeout()) options.timeout(convertDuration(getFunctionOptions.getTimeout()));
    if (getFunctionOptions.hasParentSpanId()) options.parentSpan((spans.get(getFunctionOptions.getParentSpanId())));

    return options;
  }

}
