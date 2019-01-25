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

import com.couchbase.client.java.codec.Decoder;
import com.couchbase.client.java.codec.DefaultDecoder;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Experimental prototype for a different result type on fetch.
 */
public class LookupInResult {

  private final String id;
  private final List<EncodedFragment> encoded;
  private final long cas;
  private final Optional<Duration> expiration;

  static LookupInResult create(final String id, final List<EncodedFragment> encoded,
                               final long cas, final Optional<Duration> expiration)
  {
    return new LookupInResult(id, encoded, cas, expiration);
  }

  private LookupInResult(final String id, final List<EncodedFragment> encoded, final long cas,
                         final Optional<Duration> expiration) {
    this.id = id;
    this.cas = cas;
    this.encoded = encoded;
    this.expiration = expiration;
  }

  public String id() {
    return id;
  }

  public long cas() {
    return cas;
  }

  public Optional<Duration> expiration() {
    return expiration;
  }

  @SuppressWarnings({ "unchecked" })
  public <T> T contentAs(int index, final Class<T> target) {
    return contentAs(index, target, (Decoder<T>) DefaultDecoder.INSTANCE);
  }

  public <T> T contentAs(int index, final Class<T> target, final Decoder<T> decoder) {
    return null;
  }

  public boolean exists(int index) {
    return false;
  }

  @Override
  public String toString() {
    return "LookupResult{" +
      "id='" + id + '\'' +
      ", encoded=" + encoded +
      ", cas=" + cas +
      ", expiration=" + expiration +
      '}';
  }

}
