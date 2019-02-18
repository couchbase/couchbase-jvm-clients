/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.msg.kv.SubdocField;
import com.couchbase.client.java.codec.Decoder;
import com.couchbase.client.java.codec.DefaultDecoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.List;

/**
 * Experimental prototype for a different result type on fetch.
 */
public class LookupInResult {

  private final String id;
  private final List<SubdocField> encoded;
  private final long cas;

  static LookupInResult create(final String id, final List<SubdocField> encoded,
                               final long cas) {
    return new LookupInResult(id, encoded, cas);
  }

  private LookupInResult(final String id, final List<SubdocField> encoded,
                         final long cas) {
    this.id = id;
    this.cas = cas;
    this.encoded = encoded;
  }

  public String id() {
    return id;
  }

  public long cas() {
    return cas;
  }


  @SuppressWarnings({ "unchecked" })
  public <T> T contentAs(int index, final Class<T> target) {
    return contentAs(index, target, (Decoder<T>) DefaultDecoder.INSTANCE);
  }

  public <T> T contentAs(int index, final Class<T> target, final Decoder<T> decoder) {
    if (index >= 0 && index < encoded.size()) {
      SubdocField value = encoded.get(index);
      value.error().map(err -> {
        throw err;
      });
      return decoder.decode(target, EncodedDocument.of(0, value.value()));
    }
    else {
      throw new IllegalArgumentException("Index " + index + " is invalid");
    }
  }

  public JsonObject contentAsObject(int index) {
    return contentAs(index, JsonObject.class);
  }

  public JsonArray contentAsArray(int index) {
    return contentAs(index, JsonArray.class);
  }


  public boolean exists(int index) {
    if (index >= 0 && index < encoded.size()) {
      SubdocField value = encoded.get(index);
      return value.status().success();
    }
    else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "LookupResult{" +
      "id='" + id + '\'' +
      ", encoded=" + encoded +
      ", cas=" + cas +
      '}';
  }

}
