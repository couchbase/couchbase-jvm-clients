/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import reactor.util.annotation.Nullable;

import java.io.Serializable;
import java.util.OptionalInt;
import java.util.OptionalLong;

@Stability.Internal
public final class CoreKvResponseMetadata implements Serializable {
  private final @Nullable Integer readUnits;
  private final @Nullable Integer writeUnits;
  private final @Nullable Long serverDuration;

  public static final CoreKvResponseMetadata NONE = new CoreKvResponseMetadata(-1, -1, -1);

  public static CoreKvResponseMetadata from(@Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    return flexibleExtras == null
        ? NONE
        : new CoreKvResponseMetadata(flexibleExtras.readUnits, flexibleExtras.writeUnits, flexibleExtras.serverDuration);
  }

  public static CoreKvResponseMetadata of(int readUnits, int writeUnits, long serverDuration) {
    return serverDuration < 0 && readUnits < 0 && writeUnits < 0
        ? NONE
        : new CoreKvResponseMetadata(readUnits, writeUnits, serverDuration);
  }

  private CoreKvResponseMetadata(int readUnits, int writeUnits, long serverDuration) {
    this.readUnits = readUnits < 0 ? null : readUnits;
    this.writeUnits = writeUnits < 0 ? null : writeUnits;
    this.serverDuration = serverDuration < 0 ? null : serverDuration;
  }

  public @Nullable Integer readUnits() {
    return readUnits;
  }

  public @Nullable Integer writeUnits() {
    return writeUnits;
  }

  public @Nullable Long serverDuration() {
    return serverDuration;
  }

  @Override
  public String toString() {
    return "CoreKvOpMetadata{" +
        "readUnits=" + readUnits +
        ", writeUnits=" + writeUnits +
        ", serverDuration=" + serverDuration +
        '}';
  }
}
