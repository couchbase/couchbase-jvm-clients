package com.couchbase.manager;

// [skip:<3.2.4]

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.*;
import com.couchbase.client.protocol.shared.Durability;
import com.google.protobuf.Duration;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.utils.OptionsUtil.convertDuration;

public class BucketManagerHelper {

  private BucketManagerHelper() {

  }

  public static void handleBucketManger(Cluster cluster,
                                        ConcurrentHashMap<String, RequestSpan> spans,
                                        com.couchbase.client.protocol.sdk.Command command,
                                        Result.Builder result) {

    var bm = command.getClusterCommand().getBucketManager();
    if (bm.hasGetBucket()) {
      var request = bm.getGetBucket();
      var bucketName = request.getBucketName();

      com.couchbase.client.java.manager.bucket.BucketSettings response;

      if (!request.hasOptions()) {
        response = cluster.buckets().getBucket(bucketName);
      } else {
        var options = createGetBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().getBucket(bucketName, options);
      }


      populateResult(result, response);
    }
  }

  public static Mono<Result> handleBucketManagerReactive(ReactiveCluster cluster,
                                                         ConcurrentHashMap<String, RequestSpan> spans,
                                                         com.couchbase.client.protocol.sdk.Command command,
                                                         Result.Builder result) {

    var bm = command.getClusterCommand().getBucketManager();

    if (bm.hasGetBucket()) {
      Mono<com.couchbase.client.java.manager.bucket.BucketSettings> response;
      var request = bm.getGetBucket();
      var bucketName = request.getBucketName();
      if (!request.hasOptions()) {
        response = cluster.buckets().getBucket(bucketName);
      } else {
        var options = createGetBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().getBucket(bucketName, options);
      }

      return response.map(r -> {
        populateResult(result, r);
        return result.build();
      });

    } else {
      return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
    }

  }

  public static void populateResult(Result.Builder result, com.couchbase.client.java.manager.bucket.BucketSettings response) {
    var builder = BucketSettings.newBuilder();

    builder.setBucketType(BucketType.valueOf(response.bucketType().toString()))
            .setCompressionMode(CompressionMode.valueOf(response.compressionMode().toString()))
            .setFlushEnabled(response.flushEnabled())
            .setEvictionPolicy(EvictionPolicyType.valueOf(response.evictionPolicy().toString()))
            .setMinimumDurabilityLevel(Durability.valueOf(response.minimumDurabilityLevel().toString()))
            .setName(response.name())
            .setNumReplicas(response.numReplicas())
            .setRamQuotaMB(response.ramQuotaMB())
            .setStorageBackend(StorageBackend.valueOf(response.storageBackend().toString().toUpperCase()))
            .setMaxExpiry(Duration.newBuilder().setNanos(response.maxExpiry().getNano()).setSeconds(response.maxExpiry().getSeconds()));

    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setBucketManagerResult(com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result.newBuilder()
                    .setBucketSettings(builder)));

  }

  private static com.couchbase.client.java.manager.bucket.GetBucketOptions createGetBucketOptions(GetBucketOptions getBucketOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.bucket.GetBucketOptions.getBucketOptions();

    if (getBucketOptions.hasTimeout()) options.timeout(convertDuration(getBucketOptions.getTimeout()));
    if (getBucketOptions.hasParentSpanId()) options.parentSpan(spans.get(getBucketOptions.getParentSpanId()));

    return options;
  }

}
