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
package com.couchbase.client.test;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.Objects;

public class ClusterVersion implements Comparable<ClusterVersion> {
    private final int majorVersion;
    private final int minorVersion;
    private final int patchVersion;
    private final boolean communityEdition;

    private static final Comparator<ClusterVersion> naturalOrder = Comparator
      .comparing(ClusterVersion::majorVersion)
      .thenComparing(ClusterVersion::minorVersion)
      .thenComparing(ClusterVersion::patchVersion);

    public ClusterVersion(int majorVersion, int minorVersion, int patchVersion, boolean communityEdition) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.patchVersion = patchVersion;
        this.communityEdition = communityEdition;
    }

    public int majorVersion() {
        return majorVersion;
    }

    public int minorVersion() {
        return minorVersion;
    }

    public int patchVersion() {
        return patchVersion;
    }

    /**
     * Pass "6.5.1" or "6.5.1-SNAPSHOT" get a ClusterVersion back.  Any "-SNAPSHOT" is discarded.
     */
    public static ClusterVersion parseString(String version) {
        String[] splits = version.split("\\.");
        int majorVersion = Integer.parseInt(splits[0]);
        int minorVersion = Integer.parseInt(splits[1]);
        int patchVersion = Integer.parseInt(splits[2].split("-")[0]);
        boolean communityEdition = version.contains("community");

        return new ClusterVersion(majorVersion, minorVersion, patchVersion, communityEdition);
    }

    public boolean isGreaterThan(ClusterVersion other) {
      return this.compareTo(other) > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterVersion that = (ClusterVersion) o;
        return majorVersion == that.majorVersion &&
                minorVersion == that.minorVersion &&
                patchVersion == that.patchVersion;
    }

    @Override
    public String toString() {
        return majorVersion + "." + minorVersion + "." + patchVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(majorVersion, minorVersion, patchVersion);
    }

    public boolean isCommunityEdition() {
        return communityEdition;
    }

  @Override
  public int compareTo(@NotNull ClusterVersion o) {
    return naturalOrder.compare(this, o);
  }
}
