/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class VersionAndGitHash {
  private static final Logger log = LoggerFactory.getLogger(VersionAndGitHash.class);

  private final String version;
  private final String gitHash;

  public static final VersionAndGitHash UNKNOWN = new VersionAndGitHash("0.0.0", "");

  private VersionAndGitHash(String version, String gitHash) {
    this.version = requireNonNull(version);
    this.gitHash = requireNonNull(gitHash);
  }

  public String version() {
    return version;
  }

  public String gitHash() {
    return gitHash;
  }

  @Override
  public String toString() {
    return version + " (" + gitHash + ")";
  }

  public static VersionAndGitHash from(Class<?> classInSamePackageAsVersionMetadata) {
    String resourceName = "version.properties";
    try (InputStream is = classInSamePackageAsVersionMetadata.getResourceAsStream(resourceName)) {
      if (is == null) {
        log.warn("Resource '{}' not found in package {}", resourceName, classInSamePackageAsVersionMetadata.getPackage());
        return UNKNOWN;
      }
      Properties properties = new Properties();
      properties.load(is);
      String version = properties.getProperty("version");
      String gitHash = properties.getProperty("gitHash");

      return new VersionAndGitHash(
        version == null ? "0.0.0" : version,
        gitHash == null ? "" : gitHash
      );
    } catch (IOException e) {
      log.warn("Failed to load '{}' from {}", resourceName, classInSamePackageAsVersionMetadata, e);
      return UNKNOWN;
    }
  }
}
