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
