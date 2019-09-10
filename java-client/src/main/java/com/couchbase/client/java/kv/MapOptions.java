package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

public class MapOptions extends CommonDatastructureOptions<MapOptions> {
    public static MapOptions mapOptions() { return new MapOptions(); }

    private MapOptions() {

    }

    @Stability.Internal
    public MapOptions.Built build() {
        return new MapOptions.Built();
    }

    public class Built extends BuiltCommonDatastructureOptions {


    }
}
