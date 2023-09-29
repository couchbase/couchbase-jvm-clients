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

package com.couchbase.client.core.config;

import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.NonNull;

import java.util.Comparator;
import java.util.Objects;

@Stability.Internal
public class ConfigVersion implements Comparable<ConfigVersion> {
  /**
   * A synthetic version, older than anything the server could send.
   * <p>
   * (Actually, the server could send a version with a negative epoch
   * to indicate the epoch is not yet initialized, but we want
   * to ignore those undercooked configs.)
   */
  public static final ConfigVersion ZERO = new ConfigVersion(0, 0);

  private static final Comparator<ConfigVersion> naturalOrder =
    Comparator
      .comparing(ConfigVersion::epoch)
      .thenComparing(ConfigVersion::rev);

  private final long epoch;
  private final long rev;

  /**
   * @param epoch Major version. May be negative to indicate the epoch is not yet initialized.
   * May be zero to indicate the server is too old to know about epochs.
   * @param rev Minor version. All rev values returned by the server are positive.
   */
  public ConfigVersion(long epoch, long rev) {
    this.epoch = epoch;
    this.rev = rev;
  }

  public long epoch() {
    return epoch;
  }

  public long rev() {
    return rev;
  }

  @Override
  public String toString() {
    return epoch + "." + rev;
  }

  @Override
  public int compareTo(@NonNull ConfigVersion o) {
    return naturalOrder.compare(this, o);
  }

  public boolean isLessThanOrEqualTo(ConfigVersion other) {
    return compareTo(other) < 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConfigVersion that = (ConfigVersion) o;
    return epoch == that.epoch && rev == that.rev;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, rev);
  }
}
