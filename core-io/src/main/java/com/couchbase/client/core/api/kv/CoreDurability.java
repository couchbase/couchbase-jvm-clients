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
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.service.kv.Observe;
import reactor.util.annotation.Nullable;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Either a synchronous {@link DurabilityLevel}, or "persist to & replicate to" for legacy durability.
 */
@Stability.Internal
public final class CoreDurability {
  private static final Map<DurabilityLevel, CoreDurability> cached;

  static {
    EnumMap<DurabilityLevel, CoreDurability> map = new EnumMap<>(DurabilityLevel.class);
    for (DurabilityLevel level : DurabilityLevel.values()) {
      map.put(level, new CoreDurability(level));
    }
    cached = unmodifiableMap(map);
  }

  public static final CoreDurability NONE = CoreDurability.of(DurabilityLevel.NONE);

  @Nullable private final DurabilityLevel durabilityLevel;
  @Nullable private final Observe.ObservePersistTo legacyPersistTo;
  @Nullable private final Observe.ObserveReplicateTo legacyReplicateTo;

  private CoreDurability(DurabilityLevel durabilityLevel) {
    this.durabilityLevel = requireNonNull(durabilityLevel);
    this.legacyPersistTo = null;
    this.legacyReplicateTo = null;
  }

  private CoreDurability(
      Observe.ObservePersistTo legacyPersistTo,
      Observe.ObserveReplicateTo legacyReplicateTo
  ) {
    this.durabilityLevel = null;
    this.legacyPersistTo = requireNonNull(legacyPersistTo);
    this.legacyReplicateTo = requireNonNull(legacyReplicateTo);
  }

  public static CoreDurability of(DurabilityLevel level) {
    return cached.get(level);
  }

  public static CoreDurability of(
      Observe.ObservePersistTo persistTo,
      Observe.ObserveReplicateTo replicateTo
  ) {
    return replicateTo == Observe.ObserveReplicateTo.NONE && persistTo == Observe.ObservePersistTo.NONE
        ? of(DurabilityLevel.NONE)
        : new CoreDurability(persistTo, replicateTo);
  }

  public boolean isNone() {
    return this.durabilityLevel == DurabilityLevel.NONE;
  }

  public boolean isLegacy() {
    return durabilityLevel == null;
  }

  public boolean isPersistent() {
    return durabilityLevel != null
        ? durabilityLevel == DurabilityLevel.PERSIST_TO_MAJORITY || durabilityLevel == DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
        : legacyPersistTo != Observe.ObservePersistTo.NONE;
  }

  public Optional<DurabilityLevel> levelIfSynchronous() {
    return durabilityLevel == DurabilityLevel.NONE ? Optional.empty() : Optional.ofNullable(durabilityLevel);
  }

  @Nullable
  public Observe.ObservePersistTo legacyPersistTo() {
    return legacyPersistTo;
  }

  @Nullable
  public Observe.ObserveReplicateTo legacyReplicateTo() {
    return legacyReplicateTo;
  }

  @Override
  public String toString() {
    if (durabilityLevel != null) {
      return durabilityLevel.toString();
    }
    return "LegacyDurability{" +
        "persistTo=" + legacyPersistTo +
        ", replicateTo=" + legacyReplicateTo +
        '}';
  }
}
