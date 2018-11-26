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

package com.couchbase.client.java.options;

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;

import java.time.Duration;
import java.util.function.Function;

/**
 * todo: make this extend the regular insert options.
 *
 * @param <T> the generic encoder type.
 */
public class FullInsertOptions<T> {

  public static FullInsertOptions<Object> DEFAULT = new FullInsertOptions<>();

  private Function<T, byte[]> encoder;
  private Duration timeout;
  private Duration expiry;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;

  private FullInsertOptions() { }

  public static <T> FullInsertOptions<T> insertOptions() {
    return new FullInsertOptions<>();
  }

  public Function<T, byte[]> encoder() {
    return encoder;
  }

  public FullInsertOptions<T> encoder(Function<T, byte[]> encoder) {
    this.encoder = encoder;
    return this;
  }

  public Duration timeout() {
    return timeout;
  }

  public FullInsertOptions<T> timeout(final Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public Duration expiry() {
    return expiry;
  }

  public FullInsertOptions<T> expiry(final Duration expiry) {
    this.expiry = expiry;
    return this;
  }

  public PersistTo persistTo() {
    return persistTo;
  }

  public FullInsertOptions<T> persistTo(final PersistTo persistTo) {
    this.persistTo = persistTo;
    return this;
  }

  public ReplicateTo replicateTo() {
    return replicateTo;
  }

  public FullInsertOptions<T> replicateTo(final ReplicateTo replicateTo) {
    this.replicateTo = replicateTo;
    return this;
  }

}
