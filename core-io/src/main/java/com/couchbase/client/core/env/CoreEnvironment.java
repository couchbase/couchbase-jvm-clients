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

package com.couchbase.client.core.env;

import com.couchbase.client.core.Timer;
import com.couchbase.client.core.cnc.DefaultEventBus;
import com.couchbase.client.core.cnc.EventBus;

import java.util.function.Supplier;

/**
 * The {@link CoreEnvironment} is an extendable, configurable and stateful
 * config designed to be passed into a core instance.
 *
 * @since 1.0.0
 */
public class CoreEnvironment {

  private static final Supplier<String> DEFAULT_USER_AGENT = () -> "foobar";

  private final Supplier<String> userAgent;
  private final Supplier<EventBus> eventBus;
  private final Timer timer;
  private final IoEnvironment ioEnvironment;

  protected CoreEnvironment(final Builder builder) {
    this.userAgent = builder.userAgent == null
      ? DEFAULT_USER_AGENT
      : builder.userAgent;
    this.eventBus = builder.eventBus == null
      ? (OwnedSupplier<EventBus>) DefaultEventBus::create
      : builder.eventBus;
    this.timer = builder.timer == null
      ? Timer.createAndStart()
      : builder.timer;
    this.ioEnvironment = builder.ioEnvironment == null
      ? IoEnvironment.create()
      : builder.ioEnvironment;

    if (this.eventBus instanceof OwnedSupplier) {
      ((DefaultEventBus) eventBus.get()).start();
    }
  }

  public static CoreEnvironment create() {
    return builder().build();
  }

  public static CoreEnvironment create(final String connectionString) {
    return builder()
      .load(new ConnectionStringPropertyLoader(connectionString))
      .build();
  }

  public static CoreEnvironment.Builder builder() {
    return new Builder();
  }

  /**
   * User agent used to identify this client against the server.
   *
   * @return the user agent as a string representation.
   */
  public String userAgent() {
    return userAgent.get();
  }

  /**
   * The central event bus which manages all kinds of messages flowing
   * throughout the client.
   *
   * @return the event bus currently in use.
   */
  public EventBus eventBus() {
    return eventBus.get();
  }

  /**
   * Holds the environmental configuration/state that is tied to the IO
   * layer.
   *
   * @return the IO environment currently in use.
   */
  public IoEnvironment ioEnvironment() {
    return ioEnvironment;
  }

  /**
   * Holds the timer which is used to schedule tasks and trigger their callback,
   * for example to time out requests.
   *
   * @return the timer used.
   */
  public Timer timer() {
    return timer;
  }

  public static class Builder<SELF extends Builder<SELF>> {

    private Supplier<String> userAgent = null;
    private Supplier<EventBus> eventBus = null;
    private Timer timer = null;
    private IoEnvironment ioEnvironment = null;

    @SuppressWarnings({ "unchecked" })
    protected SELF self() {
      return (SELF) this;
    }

    public SELF userAgent(final String userAgent) {
      return userAgent(() -> userAgent);
    }

    public SELF userAgent(final Supplier<String> userAgent) {
      this.userAgent = userAgent;
      return self();
    }

    public SELF load(final PropertyLoader<Builder> loader) {
      loader.load(this);
      return self();
    }

    public SELF eventBus(final EventBus eventBus) {
      return eventBus(() -> eventBus);
    }

    public SELF eventBus(final Supplier<EventBus> eventBus) {
      this.eventBus = eventBus;
      return self();
    }

    /**
     * Allows to pass in a custom {@link Timer}.
     *
     * Note that this is advanced API! Also if a timer is passed in, it needs
     * to be started manually. If this is not done it can lead to unintended
     * consequences like requests not timing out!
     *
     * @param timer the timer to use.
     * @return this build for chaining purposes.
     */
    public SELF timer(final Timer timer) {
      this.timer = timer;
      return self();
    }

    public CoreEnvironment build() {
      return new CoreEnvironment(this);
    }
  }

}
