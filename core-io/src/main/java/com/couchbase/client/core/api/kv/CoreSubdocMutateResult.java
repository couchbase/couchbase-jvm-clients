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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreSubdocMutateResult extends CoreMutationResult {
  private final List<SubDocumentField> fields;

  public CoreSubdocMutateResult(
      CoreKeyspace keyspace,
      String key,
      @Nullable CoreKvResponseMetadata meta,
      long cas,
      Optional<MutationToken> mutationToken,
      List<SubDocumentField> fields
  ) {
    super(meta, keyspace, key, cas, mutationToken);
    this.fields = requireNonNull(fields);
  }

  public SubDocumentField field(int index) {
    try {
      SubDocumentField value = fields.get(index);
      if (value == null) {
        throw new NoSuchElementException("No result exists at index " + index);
      }

      value.error().ifPresent(err -> {
        throw err;
      });

      return value;

    } catch (IndexOutOfBoundsException e) {
      throw InvalidArgumentException.fromMessage("Index " + index + " is invalid", e);
    }
  }
}
