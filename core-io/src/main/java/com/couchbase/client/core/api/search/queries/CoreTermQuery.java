/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.protostellar.search.v1.Query;
import com.couchbase.client.protostellar.search.v1.TermQuery;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreTermQuery extends CoreSearchQuery {

  private final String term;
  private final @Nullable String field;
  private final @Nullable Integer fuzziness;
  private final @Nullable Integer prefixLength;

  public CoreTermQuery(String term,
                       @Nullable String field,
                       @Nullable Integer fuzziness,
                       @Nullable Integer prefixLength,
                       @Nullable Double boost) {
    super(boost);
    this.term = notNull(term, "Term");
    this.field = field;
    this.fuzziness = fuzziness;
    this.prefixLength = prefixLength;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    input.put("term", term);
    if (field != null) {
      input.put("field", field);
    }
    if (fuzziness != null && fuzziness > 0) {
      input.put("fuzziness", fuzziness);
      if (prefixLength != null && prefixLength > 0) {
        input.put("prefix_length", prefixLength);
      }
    }
  }

  @Override
  public Query asProtostellar() {
    TermQuery.Builder builder = TermQuery.newBuilder()
            .setTerm(term);

    if (field != null) {
      builder.setField(field);
    }

    if (prefixLength != null) {
      builder.setPrefixLength(prefixLength);
    }

    if (fuzziness != null) {
      builder.setFuzziness(fuzziness);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setTermQuery(builder).build();
  }
}
