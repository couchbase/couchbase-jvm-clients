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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.CommonOptions;

import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Extends the {@link CommonOptions} to also include the durability requirements.
 *
 * @since 2.0.0
 */
abstract class CommonDurabilityOptions<SELF extends CommonDurabilityOptions<SELF>> extends CommonOptions<SELF> {

  /**
   * The custom durability persistence setting, if set.
   */
  private Optional<PersistTo> persistTo = Optional.empty();

  /**
   * The custom replication setting, if set.
   */
  private Optional<ReplicateTo> replicateTo = Optional.empty();

  /**
   * The custom enhanced durability level setting, if set.
   */
  private Optional<DurabilityLevel> durabilityLevel = Optional.empty();

  /**
   * Allows to customize the poll-based durability requirements for this operation.
   *
   * <p>Note: it is not possible to set this option and {@link #withDurabilityLevel(DurabilityLevel)} at
   * the same time.</p>
   *
   * @param persistTo the durability persistence requirement.
   * @param replicateTo the durability replication requirement.
   * @return this options builder for chaining purposes.
   */
  public SELF withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
    notNull(persistTo, "ObservePersistTo");
    notNull(replicateTo, "ObserveReplicateTo");
    if (durabilityLevel.isPresent()) {
      throw new IllegalStateException("Durability and DurabilityLevel cannot be set both at " +
        "the same time!");
    }

    this.persistTo = Optional.of(persistTo);
    this.replicateTo = Optional.of(replicateTo);
    return self();
  }

  /**
   * Allows to customize the enhanced durability requirements for this operation.
   *
   * <p>Note: it is not possible to set this option and {@link #withDurabilityLevel(DurabilityLevel)} at
   * the same time.</p>
   *
   * @param durabilityLevel the enhanced durability requirement.
   * @return this options builder for chaining purposes.
   */
  public SELF withDurabilityLevel(final DurabilityLevel durabilityLevel) {
    notNull(persistTo, "DurabilityLevel");
    if (persistTo.isPresent()|| replicateTo.isPresent()) {
      throw new IllegalStateException("Durability and DurabilityLevel cannot be set both at " +
        "the same time!");
    }

    this.durabilityLevel = Optional.of(durabilityLevel);
    return self();
  }

  public abstract class BuiltCommonDurabilityOptions extends BuiltCommonOptions {

    /**
     * Returns the persistence durability setting if provided.
     */
    public Optional<PersistTo> persistTo() {
      return persistTo;
    }

    /**
     * Returns the replication durability setting if provided.
     */
    public Optional<ReplicateTo> replicateTo() {
      return replicateTo;
    }

    /**
     * Returns the enhanced durability setting if provided.
     */
    public Optional<DurabilityLevel> durabilityLevel() {
      return durabilityLevel;
    }


  }

}
