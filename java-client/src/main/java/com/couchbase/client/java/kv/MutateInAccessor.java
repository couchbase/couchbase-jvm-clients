package com.couchbase.client.java.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.error.DefaultErrorUtil;
import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.service.kv.Observe;
import com.couchbase.client.core.service.kv.ObserveContext;

import java.util.concurrent.CompletableFuture;

public class MutateInAccessor {

  public static CompletableFuture<MutateInResult> mutateIn(final Core core,
                                                           final SubdocMutateRequest request,
                                                           final String key,
                                                           final PersistTo persistTo,
                                                           final ReplicateTo replicateTo,
                                                           final Boolean insertDocument) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        switch (response.status()) {
          case SUCCESS:
            return new MutateInResult(response.values(), response.cas(), response.mutationToken());
          case SUBDOC_FAILURE:
            throw response.error().orElse(new SubDocumentException("Unknown SubDocument error") {});
          case EXISTS:
            if (insertDocument) throw DefaultErrorUtil.docExists(key);
            else throw DefaultErrorUtil.casMismatch(key);
          default:
            throw DefaultErrorUtil.defaultErrorForStatus(key, response.status());
        }
      }).thenCompose(result -> {
        final ObserveContext ctx = new ObserveContext(
          core.context(),
          persistTo.coreHandle(),
          replicateTo.coreHandle(),
          result.mutationToken(),
          result.cas(),
          request.collectionIdentifier(),
          key,
          false,
          request.timeout()
        );
        return Observe.poll(ctx).toFuture().thenApply(v -> result);
      });
  }
}
