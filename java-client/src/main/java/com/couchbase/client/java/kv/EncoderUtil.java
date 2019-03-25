package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.codec.DefaultEncoder;
import com.couchbase.client.java.codec.Encoder;

@Stability.Internal
public class EncoderUtil {
    private EncoderUtil() {}

    public static final Encoder ENCODER = new DefaultEncoder();
}
