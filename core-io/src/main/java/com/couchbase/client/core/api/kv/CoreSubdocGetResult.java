/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import reactor.util.annotation.Nullable;

import java.util.List;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreSubdocGetResult extends CoreKvResult {
  private final long cas;
  private final boolean tombstone;
  private final List<SubDocumentField> fields;

  public CoreSubdocGetResult(
      CoreKeyspace keyspace,
      String key,
      @Nullable CoreKvResponseMetadata meta,
      List<SubDocumentField> fields,
      long cas,
      boolean tombstone
  ) {
    super(keyspace, key, meta);
    this.fields = requireNonNull(fields);
    this.cas = cas;
    this.tombstone = tombstone;
  }

  public List<SubDocumentField> fields() {
    return fields;
  }

  public SubDocumentField field(int index) {
    try {
      return fields.get(index);
    } catch (IndexOutOfBoundsException e) {
      throw InvalidArgumentException.fromMessage("Index " + index + " is out of bounds; must be >= 0 and < " + fields.size(), e);
    }
  }

  public boolean exists(int index) {
    SubDocumentField field = field(index);
    CouchbaseException error = field.error().orElse(null);

    if (error == null) {
      return true;
    }

    if (error instanceof PathNotFoundException) {
      return false;
    }

    // Propagate all other errors (path mismatch, document not json, etc.)
    throw error;
  }

  public long cas() {
    return cas;
  }

  public boolean tombstone() {
    return tombstone;
  }

  @Override
  public String toString() {
    return "CoreSubdocGetResult{" +
        "cas=" + cas +
        ", tombstone=" + tombstone +
        ", fields=" + redactUser(fields) +
        '}';
  }
}
