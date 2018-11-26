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

import java.time.Duration;

public class InsertOptions {

  public static InsertOptions DEFAULT = new InsertOptions();

  private Duration timeout;
  private Duration expiry;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;

  private InsertOptions() { }

  public static InsertOptions insertOptions() {
    return new InsertOptions();
  }


  public Duration timeout() {
    return timeout;
  }

  public InsertOptions timeout(final Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public Duration expiry() {
    return expiry;
  }

  public InsertOptions expiry(final Duration expiry) {
    this.expiry = expiry;
    return this;
  }

  public PersistTo persistTo() {
    return persistTo;
  }

  public InsertOptions persistTo(final PersistTo persistTo) {
    this.persistTo = persistTo;
    return this;
  }

  public ReplicateTo replicateTo() {
    return replicateTo;
  }

  public InsertOptions replicateTo(final ReplicateTo replicateTo) {
    this.replicateTo = replicateTo;
    return this;
  }

}
