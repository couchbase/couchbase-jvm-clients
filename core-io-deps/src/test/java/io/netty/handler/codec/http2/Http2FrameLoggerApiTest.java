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

package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.handler.codec.Headers;
import io.netty.handler.logging.LogLevel;
import io.netty.util.internal.logging.InternalLogger;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies this project's replacement Http2FrameLogger has the same API as Netty's original.
 * <p>
 * Other Netty classes (and gRPC) call the original. If a Netty upgrade changes its API,
 * the shaded jar would still build, but fail at runtime with NoSuchMethodError.
 */
class Http2FrameLoggerApiTest {

  @Test
  void replacementHasSameApiAsOriginal() throws Exception {
    try (URLClassLoader nettyOnly = new URLClassLoader(nettyJars(), null)) {
      Class<?> original = nettyOnly.loadClass(Http2FrameLogger.class.getName());
      assertFalse(original == Http2FrameLogger.class, "Failed to load Netty's original class");

      assertEquals(api(original), api(Http2FrameLogger.class));
      assertEquals(api(nettyOnly.loadClass(Http2FrameLogger.Direction.class.getName())), api(Http2FrameLogger.Direction.class));
    }
  }

  /**
   * Returns the locations of the Netty jars needed to load Netty's original Http2FrameLogger,
   * excluding this project's classes.
   */
  private static URL[] nettyJars() {
    return Stream.of(
        Http2Headers.class, // netty-codec-http2
        Headers.class, // netty-codec-base
        LogLevel.class, // netty-handler
        ChannelHandlerAdapter.class, // netty-transport
        ByteBuf.class, // netty-buffer
        InternalLogger.class // netty-common
      )
      .map(c -> c.getProtectionDomain().getCodeSource().getLocation())
      .distinct()
      .toArray(URL[]::new);
  }

  /**
   * Returns a description of the public and protected members of the given class,
   * using type names so classes from different class loaders compare equal.
   */
  private static Set<String> api(Class<?> c) {
    Set<String> result = new TreeSet<>();
    result.add("modifiers " + Modifier.toString(c.getModifiers()));
    result.add("extends " + (c.getSuperclass() == null ? null : c.getSuperclass().getName()));
    for (Class<?> i : c.getInterfaces()) {
      result.add("implements " + i.getName());
    }
    for (Constructor<?> ctor : c.getDeclaredConstructors()) {
      if (isApi(ctor)) {
        result.add(describe(ctor, "<init>", "void"));
      }
    }
    for (Method m : c.getDeclaredMethods()) {
      if (isApi(m)) {
        result.add(describe(m, m.getName(), m.getReturnType().getName()));
      }
    }
    for (Class<?> nested : c.getDeclaredClasses()) {
      if (isApi(nested.getModifiers())) {
        result.add("nested " + Modifier.toString(nested.getModifiers()) + " " + nested.getName());
      }
    }
    if (c.isEnum()) {
      for (Object constant : c.getEnumConstants()) {
        result.add("enum constant " + constant);
      }
    }
    return result;
  }

  private static boolean isApi(Executable e) {
    return !e.isSynthetic() && isApi(e.getModifiers());
  }

  private static boolean isApi(int modifiers) {
    return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
  }

  private static String describe(
    Executable e,
    String name,
    String returnType
  ) {
    String params = Arrays.stream(e.getParameterTypes())
      .map(Class::getName)
      .collect(joining(", "));
    return Modifier.toString(e.getModifiers()) + " " + returnType + " " + name + "(" + params + ")";
  }
}
