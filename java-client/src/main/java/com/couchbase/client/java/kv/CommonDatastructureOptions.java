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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.CommonOptions;

public abstract class CommonDatastructureOptions<SELF extends CommonDatastructureOptions<SELF>> extends CommonOptions<SELF> {

    private static final int DEFAULT_CAS_MISMATCH_RETRIES = 10;
    private int casMismatchRetries;

    protected CommonDatastructureOptions() {
        this.casMismatchRetries = DEFAULT_CAS_MISMATCH_RETRIES;
    }

    @Stability.Volatile
    public SELF casMismatchRetries(int retries) {
        this.casMismatchRetries = retries;
        return self();
    }

    @Stability.Internal
    public abstract class BuiltCommonDatastructureOptions extends BuiltCommonOptions {

        public int casMismatchRetries() {
            return casMismatchRetries;
        }

        // We need to create various specific Options, which use the CommonOptions base, so
        // lets do that here
        public LookupInOptions lookupInOptions() {
            return LookupInOptions.lookupInOptions()
                    .retryStrategy(this.retryStrategy().orElse(null))
                    .clientContext(this.clientContext())
                    .timeout(this.timeout().orElse(null));
        }

        public GetOptions getOptions() {
            return GetOptions.getOptions()
                    .retryStrategy(this.retryStrategy().orElse(null))
                    .clientContext(this.clientContext())
                    .timeout(this.timeout().orElse(null));
        }

        public MutateInOptions mutateInOptions() {
            return MutateInOptions.mutateInOptions()
                    .retryStrategy(this.retryStrategy().orElse(null))
                    .clientContext(this.clientContext())
                    .timeout(this.timeout().orElse(null));

        }

        public InsertOptions insertOptions() {
            return InsertOptions.insertOptions()
                    .retryStrategy(this.retryStrategy().orElse(null))
                    .clientContext(this.clientContext())
                    .timeout(this.timeout().orElse(null));
        }

        public UpsertOptions upsertOptions() {
            return UpsertOptions.upsertOptions()
                    .retryStrategy(this.retryStrategy().orElse(null))
                    .clientContext(this.clientContext())
                    .timeout(this.timeout().orElse(null));
        }

        public void copyInto(CommonDatastructureOptions<?> c) {
            c.retryStrategy(this.retryStrategy().orElse(null));
            c.timeout(this.timeout().orElse(null));
            c.clientContext(this.clientContext());
        }
    }
}
