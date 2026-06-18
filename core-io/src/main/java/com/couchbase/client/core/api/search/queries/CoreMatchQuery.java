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
import org.jspecify.annotations.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreMatchQuery extends CoreSearchQuery {

    public final String match;
    public @Nullable final String field;
    public @Nullable final String analyzer;
    public @Nullable final Integer prefixLength;
    public @Nullable final Integer fuzziness;
    public @Nullable final CoreMatchOperator operator;

    public CoreMatchQuery(String match,
                          @Nullable String field,
                          @Nullable String analyzer,
                          @Nullable Integer prefixLength,
                          @Nullable Integer fuzziness,
                          @Nullable CoreMatchOperator operator,
                          @Nullable Double boost) {
        super(boost);
        this.match = notNull(match, "Match");
        this.field = field;
        this.analyzer = analyzer;
        this.prefixLength = prefixLength;
        this.fuzziness = fuzziness;
        this.operator = operator;
    }

    @Override
    protected void injectParams(ObjectNode input) {
        input.put("match", match);
        if (field != null) {
            input.put("field", field);
        }
        if (analyzer != null) {
            input.put("analyzer", analyzer);
        }
        if (prefixLength != null) {
            input.put("prefix_length", prefixLength);
        }
        if (fuzziness != null) {
            input.put("fuzziness", fuzziness);
        }
        if (operator != null) {
            input.put("operator", operator.toString());
        }
    }

    @Override
    public <T> T convert(CoreSearchQueryConverter<T> converter) {
        return converter.convert(this);
    }
}
