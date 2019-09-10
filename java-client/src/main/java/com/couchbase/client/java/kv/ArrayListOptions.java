package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

public class ArrayListOptions extends CommonDatastructureOptions<ArrayListOptions> {
    public static ArrayListOptions arrayListOptions() { return new ArrayListOptions(); }

    private ArrayListOptions() {

    }

    @Stability.Internal
    public Built build() {
        return new Built();
    }

    public class Built extends BuiltCommonDatastructureOptions {


    }
}
