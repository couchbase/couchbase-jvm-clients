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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LibcTest {

  @Test
  void recognizesMuslMappings() {
    assertTrue(Libc.isMuslMapping("7f8c1a228000-7f8c1a3bd000 r-xp 00028000 08:01 1835023    /lib/ld-musl-x86_64.so.1"));
    assertTrue(Libc.isMuslMapping("7f8c1a228000-7f8c1a3bd000 r-xp 00028000 08:01 1835023    /lib/ld-musl-aarch64.so.1"));
    assertTrue(Libc.isMuslMapping("7f8c1a228000-7f8c1a3bd000 r-xp 00028000 08:01 1835023    /usr/lib/libc.musl-x86_64.so.1"));
    assertTrue(Libc.isMuslMapping("7f8c1a228000-7f8c1a3bd000 r-xp 00028000 08:01 1835023    /lib/ld-musl-x86_64.so.1 (deleted)"));
  }

  @Test
  void ignoresOtherMappings() {
    assertFalse(Libc.isMuslMapping("7f8c1a200000-7f8c1a228000 r--p 00000000 08:01 1835023    /usr/lib/x86_64-linux-gnu/libc.so.6"));
    assertFalse(Libc.isMuslMapping("7f8c1a200000-7f8c1a228000 r--p 00000000 08:01 1835023    /lib64/ld-linux-x86-64.so.2"));
    assertFalse(Libc.isMuslMapping("7f8c1a5f0000-7f8c1a5f2000 rw-p 00000000 00:00 0"));
    assertFalse(Libc.isMuslMapping("7ffd3c9e1000-7ffd3ca02000 rw-p 00000000 00:00 0                          [stack]"));
    assertFalse(Libc.isMuslMapping(""));

    // "musl" in a directory name or an unrelated library name doesn't count
    assertFalse(Libc.isMuslMapping("7f8c1a200000-7f8c1a228000 r-xp 00000000 08:01 1835023    /opt/zulu21-linux_musl_x64/lib/server/libjvm.so"));
    assertFalse(Libc.isMuslMapping("7f8c1a200000-7f8c1a228000 r-xp 00000000 08:01 1835023    /tmp/libfoo-linux-x86_64-musl.so"));
  }

  @Test
  void detectsMuslFromMapsFile(@TempDir Path dir) throws IOException {
    Path glibc = Files.write(dir.resolve("glibc-maps"), asList(
      "7f8c1a200000-7f8c1a228000 r--p 00000000 08:01 1835023    /usr/lib/x86_64-linux-gnu/libc.so.6",
      "7f8c1a5f0000-7f8c1a5f2000 rw-p 00000000 00:00 0"
    ));
    assertFalse(Libc.detectMusl(glibc));

    Path musl = Files.write(dir.resolve("musl-maps"), asList(
      "7f8c1a5f0000-7f8c1a5f2000 rw-p 00000000 00:00 0",
      "7f8c1a228000-7f8c1a3bd000 r-xp 00028000 08:01 1835023    /lib/ld-musl-x86_64.so.1"
    ));
    assertTrue(Libc.detectMusl(musl));

    assertFalse(Libc.detectMusl(dir.resolve("does-not-exist")));
  }

  @Test
  void runtimeExceptionMeansNotMusl(@TempDir Path dir) throws IOException {
    // A path from a closed file system throws ClosedFileSystemException (a RuntimeException),
    // standing in for a SecurityException from a SecurityManager.
    URI zipUri = URI.create("jar:" + dir.resolve("maps.zip").toUri());
    Path maps;
    try (FileSystem zip = FileSystems.newFileSystem(zipUri, singletonMap("create", "true"))) {
      maps = zip.getPath("maps");
    }
    assertFalse(Libc.detectMusl(maps));
  }
}
