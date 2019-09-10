package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

public class ArraySetOptions extends CommonDatastructureOptions<ArrayListOptions> {
public static ArraySetOptions arraySetOptions() { return new ArraySetOptions(); }

    private ArraySetOptions() {

    }

    @Stability.Internal
    public Built build() {
        return new Built();
    }

    public class Built extends BuiltCommonDatastructureOptions {


    }
}