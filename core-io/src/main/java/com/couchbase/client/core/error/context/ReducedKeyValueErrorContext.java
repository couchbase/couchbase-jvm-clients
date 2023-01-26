/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.error.context;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

@Stability.Uncommitted
public class ReducedKeyValueErrorContext extends ErrorContext {

  private final String id;
  private final CollectionIdentifier collectionIdentifier;

  protected ReducedKeyValueErrorContext(final String id, final CollectionIdentifier collectionIdentifier) {
    super(null);
    this.id = id;
    this.collectionIdentifier = collectionIdentifier;
  }

  public static ReducedKeyValueErrorContext create(final String id, final CollectionIdentifier collectionIdentifier) {
    return new ReducedKeyValueErrorContext(id, collectionIdentifier);
  }

  public static ReducedKeyValueErrorContext create(final String id, final String bucket, final String scope, final String collection) {
    return new ReducedKeyValueErrorContext(id, new CollectionIdentifier(bucket, Optional.ofNullable(scope), Optional.ofNullable(collection)));
  }

  public static ReducedKeyValueErrorContext create(final String id) {
    return new ReducedKeyValueErrorContext(id, null);
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
