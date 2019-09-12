package com.couchbase.client.java.kv;

import com.couchbase.client.java.CommonOptions;

public abstract class CommonDatastructureOptions<SELF extends CommonDatastructureOptions<SELF>> extends CommonOptions<SELF> {

    private static final int DEFAULT_CAS_MISMATCH_RETRIES = 10;
    private int casMismatchRetries;

    protected CommonDatastructureOptions() {
        this.casMismatchRetries = DEFAULT_CAS_MISMATCH_RETRIES;
    }

    public SELF casMismatchRetries(int retries) {
        this.casMismatchRetries = retries;
        return self();
    }

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
