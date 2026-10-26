/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Detects which C standard library the JVM process is linked against.
 */
@Stability.Internal
public final class Libc {
  private static final Logger log = LoggerFactory.getLogger(Libc.class);

  /**
   * Matches the file name of musl's dynamic loader, which is also the C library
   * (for example, "ld-musl-x86_64.so.1"), or the "libc.musl-*" alias some distros use.
   */
  private static final Pattern MUSL_FILE_NAME = Pattern.compile("(ld-musl|libc\\.musl)-.+\\.so.*");

  private static final boolean MUSL = detectMusl(Paths.get("/proc/self/maps"));

  private Libc() {
  }

  /**
   * Returns true if this JVM process is linked against musl libc (for example, on Alpine Linux).
   * <p>
   * Checks the libraries actually mapped into this process, so it is not fooled by
   * musl being installed on a glibc system (or vice versa).
   * Returns false if the libc can't be determined (for example, on a non-Linux OS).
   */
  public static boolean isMusl() {
    return MUSL;
  }

  static boolean detectMusl(Path procSelfMaps) {
    // Also catch RuntimeException (for example, SecurityException from a SecurityManager),
    // since a failure here would break the static init of every class that calls isMusl().
    try {
      if (!Files.isReadable(procSelfMaps)) {
        return false;
      }

      // ISO-8859-1 decodes any byte sequence, so paths that aren't valid UTF-8 can't cause a failure.
      try (Stream<String> lines = Files.lines(procSelfMaps, ISO_8859_1)) {
        return lines.anyMatch(Libc::isMuslMapping);
      }
    } catch (IOException | RuntimeException e) {
      log.debug("Failed to read {}; assuming libc is not musl.", procSelfMaps, e);
      return false;
    }
  }

  /**
   * @param line a line from /proc/[pid]/maps, like
   * "7f8c1a228000-7f8c1a3bd000 r-xp 00028000 08:01 1835023    /lib/ld-musl-x86_64.so.1"
   */
  static boolean isMuslMapping(String line) {
    // Fields are: address, perms, offset, dev, inode, pathname (optional; may contain spaces)
    String[] fields = line.trim().split("\\s+", 6);
    if (fields.length < 6) {
      return false;
    }
    String path = fields[5];
    String fileName = path.substring(path.lastIndexOf('/') + 1);
    return MUSL_FILE_NAME.matcher(fileName).matches();
  }
}
