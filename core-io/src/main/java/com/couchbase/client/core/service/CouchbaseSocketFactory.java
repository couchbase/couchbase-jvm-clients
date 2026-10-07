/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.util.StorageSize;
import com.couchbase.client.core.util.CbDurations;
import com.couchbase.client.socketoptions.ExtendedSocket;
import com.couchbase.client.socketoptions.ExtendedSocketFactory;
import com.couchbase.client.socketoptions.LinuxSocketOptions;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.time.Duration;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Creates the TCP sockets for {@link CouchbaseOkHttpClient}'s connections (OkHttp layers TLS over them), with the
 * socket options from {@link IoConfig}, like the Netty endpoints:
 * <ul>
 *   <li>TCP_NODELAY, always.
 *   <li>TCP keepalive, unless keepalives are disabled, with the keepalive idle time, interval, and count where the
 *   JDK supports them (Java 11 or later, or a recent Java 8 update).
 *   <li>TCP_USER_TIMEOUT (unless it's zero), with the couchbase-socket-options library's native code, on Linux
 *   (x86_64 or aarch64), unless native IO is disabled.
 *   <li>SO_SNDBUF and SO_RCVBUF, if configured. Unlike the others, they're set before the socket connects:
 *   the receive buffer size affects the TCP window scale, which is agreed when connecting.
 * </ul>
 * The options are set as soon as each socket connects (before {@code connect} returns, so before the TLS handshake),
 * and each one is best-effort: if it can't be set, the OS default applies. Each socket's options (and any that
 * couldn't be set) are logged at debug level.
 * <p>
 * Thread-safe.
 */
@NullMarked
final class CouchbaseSocketFactory extends SocketFactory {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseSocketFactory.class);

  // The keepalive options from jdk.net.ExtendedSocketOptions, or null if this JDK doesn't have them. They're set with
  // Socket.setOption on Java 9+, or jdk.net.Sockets.setOption on Java 8. (This code is compiled for Java 8, so it
  // looks them up at runtime.)
  private static final @Nullable SocketOption<?> TCP_KEEPIDLE = jdkExtendedOption("TCP_KEEPIDLE");
  private static final @Nullable SocketOption<?> TCP_KEEPINTERVAL = jdkExtendedOption("TCP_KEEPINTERVAL");
  private static final @Nullable SocketOption<?> TCP_KEEPCOUNT = jdkExtendedOption("TCP_KEEPCOUNT");
  private static final @Nullable Method MODERN_SET_OPTION = method("java.net.Socket", "setOption", SocketOption.class, Object.class);
  private static final @Nullable Method JDK8_SET_OPTION = method("jdk.net.Sockets", "setOption", Socket.class, SocketOption.class, Object.class);

  private final boolean keepAlive;

  // Like the Netty endpoints, zero means "use the OS default".
  private final int keepAliveIdleSeconds;
  private final int keepAliveIntervalSeconds;
  private final int keepAliveCount;

  /**
   * The TCP_USER_TIMEOUT to set, or null to use the OS default.
   */
  private final @Nullable Duration tcpUserTimeout;

  /**
   * Creates the sockets if TCP_USER_TIMEOUT is set (with the native library), or null.
   */
  private final @Nullable ExtendedSocketFactory extendedSocketFactory;

  /**
   * The socket buffer sizes to set, or null to use the OS defaults.
   */
  private final @Nullable Integer sendBufferBytes;
  private final @Nullable Integer receiveBufferBytes;

  /**
   * Whether a socket has already failed to get TCP_USER_TIMEOUT (and that's been logged at warning level).
   */
  private final AtomicBoolean warnedAboutUserTimeoutFailure = new AtomicBoolean();

  CouchbaseSocketFactory(IoConfig ioConfig, boolean nativeIoEnabled) {
    keepAlive = ioConfig.tcpKeepAlivesEnabled();
    keepAliveIdleSeconds = keepAlive ? seconds(ioConfig.tcpKeepAliveTime()) : 0;
    keepAliveIntervalSeconds = keepAlive ? seconds(ioConfig.tcpKeepAliveInterval()) : 0;
    keepAliveCount = keepAlive ? ioConfig.tcpKeepAliveCount() : 0;
    StorageSize sendBuffer = ioConfig.sendBuffer();
    StorageSize receiveBuffer = ioConfig.receiveBuffer();
    sendBufferBytes = sendBuffer == null ? null : sendBuffer.bytesAsInt();
    receiveBufferBytes = receiveBuffer == null ? null : receiveBuffer.bytesAsInt();

    // Only loads the native library if it's needed (LinuxSocketOptions.isAvailable() loads it), and never if native
    // IO is disabled. On Java 24 and later, the JVM warns about loading it unless native access is enabled (as for
    // Netty's epoll transport).
    Duration userTimeout = ioConfig.tcpUserTimeout();
    boolean setUserTimeout = nativeIoEnabled && !userTimeout.isZero() && LinuxSocketOptions.isAvailable();
    tcpUserTimeout = setUserTimeout ? userTimeout : null;
    extendedSocketFactory = setUserTimeout ? ExtendedSocketFactory.builder().afterConnect(this::configure).build() : null;

    if (!userTimeout.isZero() && !setUserTimeout) {
      if (!nativeIoEnabled) {
        log.warn("Can't set the TCP_USER_TIMEOUT socket option, so the OS default applies. Reason: native IO is disabled.");
      } else {
        Throwable cause = LinuxSocketOptions.unavailabilityCause().orElse(null);
        String message = "Can't set TCP_USER_TIMEOUT socket option, so the OS default applies. Reason: " + cause;
        if (isLinux()) {
          log.warn(message, cause); // the stack trace may help explain why the native library didn't load
        } else {
          log.info(message); // only info: it's only supported on Linux, and it's set by default
        }
      }
    }
  }

  @Override
  public Socket createSocket() throws IOException {
    // This is the one OkHttp uses: it connects the socket itself, and the options are set once it's connected
    // (except the buffer sizes, which are set now).
    Socket socket = extendedSocketFactory != null ? extendedSocketFactory.createSocket() : new ConfiguringSocket();
    setBufferSizes(socket);
    return socket;
  }

  /**
   * Sets the configured buffer sizes on a new socket, before it connects, best-effort. The debug message logged
   * once it's connected reports the sizes it ended up with (the OS may adjust them: Linux doubles them, for example).
   */
  private void setBufferSizes(Socket socket) {
    Integer send = sendBufferBytes;
    if (send != null) {
      try {
        socket.setSendBufferSize(send);
      } catch (SocketException | IllegalArgumentException e) {
        log.debug("Failed to set SO_SNDBUF={} on a new socket", send, e);
      }
    }
    Integer receive = receiveBufferBytes;
    if (receive != null) {
      try {
        socket.setReceiveBufferSize(receive);
      } catch (SocketException | IllegalArgumentException e) {
        log.debug("Failed to set SO_RCVBUF={} on a new socket", receive, e);
      }
    }
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return connected(new InetSocketAddress(host, port), null);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
    return connected(new InetSocketAddress(host, port), new InetSocketAddress(localHost, localPort));
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return connected(new InetSocketAddress(host, port), null);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
    return connected(new InetSocketAddress(address, port), new InetSocketAddress(localAddress, localPort));
  }

  private Socket connected(InetSocketAddress remote, @Nullable InetSocketAddress local) throws IOException {
    Socket socket = createSocket();
    try {
      if (local != null) {
        socket.bind(local);
      }
      socket.connect(remote);
      return socket;
    } catch (IOException | RuntimeException e) {
      socket.close();
      throw e;
    }
  }

  /**
   * A plain socket that sets the options as soon as it connects.
   */
  private final class ConfiguringSocket extends Socket {
    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
      super.connect(endpoint, timeout);
      configure(this);
    }
  }

  /**
   * Sets the options on a newly connected socket, best-effort, and logs them.
   */
  private void configure(Socket socket) {
    StringJoiner options = new StringJoiner(", ");
    set(options, "TCP_NODELAY", true, () -> socket.setTcpNoDelay(true));
    if (keepAlive) {
      set(options, "SO_KEEPALIVE", true, () -> socket.setKeepAlive(true));
      if (keepAliveIdleSeconds != 0) set(options, "TCP_KEEPIDLE", keepAliveIdleSeconds, () -> setJdkOption(socket, TCP_KEEPIDLE, keepAliveIdleSeconds));
      if (keepAliveIntervalSeconds != 0) set(options, "TCP_KEEPINTERVAL", keepAliveIntervalSeconds, () -> setJdkOption(socket, TCP_KEEPINTERVAL, keepAliveIntervalSeconds));
      if (keepAliveCount != 0) set(options, "TCP_KEEPCOUNT", keepAliveCount, () -> setJdkOption(socket, TCP_KEEPCOUNT, keepAliveCount));
    }
    Duration userTimeout = tcpUserTimeout;
    if (userTimeout != null && socket instanceof ExtendedSocket) {
      Throwable failure = set(options, "TCP_USER_TIMEOUT", userTimeout.toMillis() + "ms", () -> ((ExtendedSocket) socket).linuxOptions()
        .orElseThrow(() -> new UnsupportedOperationException("not available"))
        .setTcpUserTimeout(userTimeout));
      // Warn once: if the cause is persistent (for example, a SOCKS proxy), every connection fails the same way.
      if (failure != null && !warnedAboutUserTimeoutFailure.getAndSet(true)) {
        log.warn("Can't set TCP_USER_TIMEOUT on {}, so the OS default applies: {} (further failures are logged at debug level)",
          socket, failure.toString());
      }
    }
    if (sendBufferBytes != null) {
      options.add("SO_SNDBUF=" + bufferSize(socket::getSendBufferSize) + " (requested " + sendBufferBytes + ")");
    }
    if (receiveBufferBytes != null) {
      options.add("SO_RCVBUF=" + bufferSize(socket::getReceiveBufferSize) + " (requested " + receiveBufferBytes + ")");
    }
    log.debug("Socket options for {}: {}", socket, options);
  }

  @FunctionalInterface
  private interface BufferSizeGetter {
    int get() throws SocketException;
  }

  private static String bufferSize(BufferSizeGetter getter) {
    try {
      return String.valueOf(getter.get());
    } catch (SocketException e) {
      return "unknown (" + e + ")";
    }
  }

  @FunctionalInterface
  private interface Setter {
    void set() throws Exception;
  }

  /**
   * Sets an option, and adds it to the options to log (noting if it couldn't be set).
   *
   * @return why it couldn't be set, or null if it was
   */
  private static @Nullable Throwable set(StringJoiner options, String name, Object value, Setter setter) {
    try {
      setter.set();
      options.add(name + "=" + value);
      return null;
    } catch (Exception e) {
      Throwable cause = e instanceof InvocationTargetException && e.getCause() != null ? e.getCause() : e;
      options.add(name + "=" + value + " (failed: " + cause + ")");
      return cause;
    }
  }

  private static void setJdkOption(Socket socket, @Nullable SocketOption<?> option, int value) throws Exception {
    if (option == null) {
      throw new UnsupportedOperationException("this JDK doesn't have it (it needs Java 11 or later, or a recent Java 8 update)");
    }
    if (MODERN_SET_OPTION != null) {
      MODERN_SET_OPTION.invoke(socket, option, value);
    } else if (JDK8_SET_OPTION != null) {
      JDK8_SET_OPTION.invoke(null, socket, option, value);
    } else {
      throw new UnsupportedOperationException("this JDK can't set extended socket options");
    }
  }

  private static @Nullable SocketOption<?> jdkExtendedOption(String name) {
    try {
      return (SocketOption<?>) Class.forName("jdk.net.ExtendedSocketOptions").getField(name).get(null);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return null; // not in this JDK
    }
  }

  private static @Nullable Method method(String className, String name, Class<?>... parameterTypes) {
    try {
      return Class.forName(className).getMethod(name, parameterTypes);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return null; // not in this JDK
    }
  }

  private static int seconds(Duration duration) {
    return Math.toIntExact(CbDurations.getSecondsCeil(duration));
  }

  private static boolean isLinux() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("linux");
  }
}
