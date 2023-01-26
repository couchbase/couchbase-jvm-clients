/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static org.junit.Assert.assertEquals;

/**
 * Tests the get-with-projection feature.
 *
 * @since 3.0.0
 */
@IgnoreWhen(isProtostellarWillWorkLater = true)
class GetProjectionIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;
  private static final String JSON_RAW = "{\n" +
          "    \"name\": \"Emmy-lou Dickerson\",\n" +
          "    \"age\": 26,\n" +
          "    \"animals\": [\"cat\", \"dog\", \"parrot\"],\n" +
          "    \"attributes\": {\n" +
          "        \"hair\": \"brown\",\n" +
          "        \"dimensions\": {\n" +
          "            \"height\": 67,\n" +
          "            \"weight\": 175\n" +
          "        },\n" +
          "        \"hobbies\": [{\n" +
          "                \"type\": \"winter sports\",\n" +
          "                \"name\": \"curling\"\n" +
          "            },\n" +
          "            {\n" +
          "                \"type\": \"summer sports\",\n" +
          "                \"name\": \"water skiing\",\n" +
          "                \"details\": {\n" +
          "                    \"location\": {\n" +
          "                        \"lat\": 49.282730,\n" +
          "                        \"long\": -123.120735\n" +
          "                    }\n" +
          "                }\n" +
          "            }\n" +
          "        ]\n" +
          "    }\n" +
          "}\n";
  private static final JsonObject JSON = JsonObject.fromJson(JSON_RAW);
  private static final String DOC_ID = "get-projection-test";

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    collection.upsert(DOC_ID, JSON);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  void name() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("name")).contentAsObject();
    assertEquals("Emmy-lou Dickerson", decoded.getString("name"));
    assertEquals(1, decoded.size());
  }

  @Test
  void age() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("age")).contentAsObject();
    assertEquals(26, decoded.getInt("age").intValue());
    assertEquals(1, decoded.size());
  }

  @Test
  void animals() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("animals")).contentAsObject();
    JsonArray arr = decoded.getArray("animals");
    assertEquals(3, arr.size());
    assertEquals(1, decoded.size());
  }

  @Test
  void animals_0() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("animals[0]")).contentAsObject();
    JsonArray arr = decoded.getArray("animals");
    assertEquals(1, arr.size());
    assertEquals("cat", arr.get(0));
    assertEquals(1, decoded.size());
  }

  @Test
  void animals_1() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("animals[1]")).contentAsObject();
    JsonArray arr = decoded.getArray("animals");
    assertEquals(1, arr.size());
    assertEquals("dog", arr.get(0));
    assertEquals(1, decoded.size());
  }

  @Test
  void animals_2() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("animals[2]")).contentAsObject();
    JsonArray arr = decoded.getArray("animals");
    assertEquals(1, arr.size());
    assertEquals("parrot", arr.get(0));
    assertEquals(1, decoded.size());
  }

  @Test
  void attributes() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes")).contentAsObject();
    JsonObject obj = decoded.getObject("attributes");
    assertEquals(3, obj.size());
    assertEquals(1, decoded.size());
  }

  @Test
  void attributes_hair() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.hair")).contentAsObject();
    JsonObject obj = decoded.getObject("attributes");
    assertEquals(1, obj.size());
    assertEquals("brown", obj.getString("hair"));
    assertEquals(1, decoded.size());
  }

  @Test
  void attributes_dimensions() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.dimensions")).contentAsObject();
    JsonObject obj = decoded.getObject("attributes").getObject("dimensions");
    assertEquals(2, obj.size());
    assertEquals(67, obj.getInt("height").intValue());
    assertEquals(175, obj.getInt("weight").intValue());
    assertEquals(1, decoded.size());
  }

  @Test
  void attributes_dimensions_height() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.dimensions.height")).contentAsObject();
    JsonObject obj = decoded.getObject("attributes").getObject("dimensions");
    assertEquals(1, obj.size());
    assertEquals(67, obj.getInt("height").intValue());
    assertEquals(1, decoded.size());
  }

  @Test
  void attributes_dimensions_weight() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.dimensions.weight")).contentAsObject();
    JsonObject obj = decoded.getObject("attributes").getObject("dimensions");
    assertEquals(1, obj.size());
    assertEquals(175, obj.getInt("weight").intValue());
    assertEquals(1, decoded.size());
  }

  @Test
  void attributes_hobbies_0_type() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.hobbies[0].type")).contentAsObject();
    JsonArray arr = decoded.getObject("attributes").getArray("hobbies");
    assertEquals(1, arr.size());
    assertEquals("winter sports", arr.getObject(0).getString("type"));
    assertEquals(1, decoded.size());
  }

  @Test
  void attributes_hobbies_1_type() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.hobbies[1].type")).contentAsObject();
    JsonArray arr = decoded.getObject("attributes").getArray("hobbies");
    assertEquals(1, arr.size());
    assertEquals("summer sports", arr.getObject(0).getString("type"));
    assertEquals(1, decoded.size());
  }

  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  @Test
  void attributes_hobbies_1_details_location() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.hobbies[1].details.location")).contentAsObject();
    JsonArray arr = decoded.getObject("attributes").getArray("hobbies");
    JsonObject obj = arr.getObject(0).getObject("details").getObject("location");
    assertEquals(1, arr.size());
    assertEquals(2, obj.size());
    assertEquals(1, decoded.size());
  }

  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  @Test
  void attributes_hobbies_1_details_location_lat() {
    JsonObject decoded = collection.get(DOC_ID, getOptions().project("attributes.hobbies[1].details.location.lat")).contentAsObject();
    JsonArray arr = decoded.getObject("attributes").getArray("hobbies");
    JsonObject obj = arr.getObject(0).getObject("details").getObject("location");
    assertEquals(1, arr.size());
    assertEquals(1, obj.size());
    assertEquals(49.282730, obj.getNumber("lat").doubleValue(), 0.1);
    assertEquals(1, decoded.size());
  }
}
