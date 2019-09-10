package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

public class QueueOptions extends CommonDatastructureOptions<QueueOptions> {
    public static QueueOptions queueOptions() { return new QueueOptions(); }

    private QueueOptions() {

    }

    @Stability.Internal
    public QueueOptions.Built build() {
        return new QueueOptions.Built();
    }

    public class Built extends BuiltCommonDatastructureOptions {


    }
}
