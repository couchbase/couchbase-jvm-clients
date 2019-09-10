package com.couchbase.client.java.kv;

import com.couchbase.client.java.CommonOptions;

public class CommonDatastructureOptions<SELF extends CommonDatastructureOptions<SELF>> extends CommonOptions<SELF> {

    private static final int DEFAULT_CAS_MISMATCH_RETRIES = 10;
    private int casMismatchRetries;

    protected CommonDatastructureOptions() {
        this.casMismatchRetries = DEFAULT_CAS_MISMATCH_RETRIES;
    }

    public static CommonDatastructureOptions datastructureOptions() {
        return new CommonDatastructureOptions();
    }

    public CommonDatastructureOptions casMismatchRetries(int retries) {
        this.casMismatchRetries = retries;
        return this;
    }

    public abstract class BuiltCommonDatastructureOptions extends BuiltCommonOptions {
        public int casMismatchRetries() {
            return casMismatchRetries;
        }
    }
}
