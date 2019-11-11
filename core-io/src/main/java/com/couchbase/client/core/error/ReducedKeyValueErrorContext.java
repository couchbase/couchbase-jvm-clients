package com.couchbase.client.core.error;

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;

import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ReducedKeyValueErrorContext extends ErrorContext {

  private final String id;
  private final CollectionIdentifier collectionIdentifier;

  private ReducedKeyValueErrorContext(final String id, final CollectionIdentifier collectionIdentifier) {
    super(null);
    this.id = id;
    this.collectionIdentifier = collectionIdentifier;
  }

  public static ReducedKeyValueErrorContext create(final String id, final CollectionIdentifier collectionIdentifier) {
    return new ReducedKeyValueErrorContext(id, collectionIdentifier);
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (id != null && !id.isEmpty()) {
      input.put("documentId", redactUser(id));
    }
    if (collectionIdentifier != null) {
      input.put("bucket", redactMeta(collectionIdentifier.bucket()));
      input.put("scope", redactMeta(collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE)));
      input.put("collection", redactMeta(collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION)));
    }
  }

}
