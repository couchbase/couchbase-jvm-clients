package com.couchbase.client.scala;

import com.couchbase.client.scala.json.JsonObject;
import org.junit.jupiter.api.Test;

public class JsonObjectTest {
    // Was using some Java keywords in the API for a while
    @Test
    public void testCompatibility() {
        JsonObject json = JsonObject.create();
        json.bool("test");
//        json.int("test");
//        json.double("test");
//        json.long("test");
//        json.obj("test");
    }
}
