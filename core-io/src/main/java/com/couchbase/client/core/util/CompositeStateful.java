/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Represents a stateful component of one or more individual stateful elements.
 * <p>
 * Every change to the composite state is serialized by this object's monitor, whether it is caused by
 * registering or deregistering an element, or by an element changing state.
 * <p>
 * Elements usually notify the composite while holding their own {@link SingleStateful} monitor,
 * so the lock order is: element, then composite, then the composite's own state.
 * Code holding this composite's monitor must therefore never wait for an element's monitor.
 * <p>
 * An element's state change may instead be delivered by the thread registering that element (see
 * {@link SingleStateful}). That thread already holds this composite's monitor, and does not hold the
 * element's monitor, so this does not violate the lock order.
 */
public class CompositeStateful<T, IN, OUT> implements Stateful<OUT> {

  private final OUT initialState;
  private final SingleStateful<OUT> inner;
  private final Function<Collection<IN>, OUT> transformer;

  /**
   * Latest known state of each registered element. Guarded by {@code this}.
   * <p>
   * Always has the same keys as {@link #subscriptions}.
   */
  private final Map<T, IN> states = new HashMap<>();

  /**
   * Subscription to each registered element's state stream. Guarded by {@code this}.
   * <p>
   * Each registration gets a new instance, so signals that were already in flight when an element
   * was deregistered (or registered again) can be recognized and ignored.
   */
  private final Map<T, Disposable.Swap> subscriptions = new HashMap<>();

  /**
   * Guarded by {@code this}.
   */
  private boolean closed;

  private CompositeStateful(final OUT initialState, final Function<Collection<IN>, OUT> transformer,
                            final BiConsumer<OUT, OUT> beforeTransitionCallback) {
    this.inner = SingleStateful.fromInitial(initialState, beforeTransitionCallback);
    this.initialState = initialState;
    this.transformer = transformer;
  }

  /**
   * Creates a new transformer with an initial state and the transform function that should be applied.
   *
   * @param initialState the initial state.
   * @param transformer the custom transformer for the states.
   * @return a created stateful composite.
   */
  public static <T, IN, OUT> CompositeStateful<T, IN, OUT> create(final OUT initialState,
                                                                  final Function<Collection<IN>, OUT> transformer,
                                                                  final BiConsumer<OUT, OUT> beforeTransitionCallback) {
    return new CompositeStateful<>(initialState, transformer, beforeTransitionCallback);
  }

  /**
   * Creates a new transformer with an initial state and the transform function that should be applied.
   *
   * @param initialState the initial state.
   * @param transformer the custom transformer for the states.
   * @return a created stateful composite.
   */
  public static <T, IN, OUT> CompositeStateful<T, IN, OUT> create(final OUT initialState,
                                                                  final Function<Collection<IN>, OUT> transformer) {
    return create(initialState, transformer, (oldState, newState) -> {});
  }

  /**
   * Registers a stateful element with the composite.
   * <p>
   * If an element is already registered with the same identifier, it is replaced.
   * Does nothing if the composite is closed.
   * <p>
   * The element is deregistered automatically when its state stream terminates.
   *
   * @param identifier the unique identifier to use.
   * @param upstream the upstream flux with the state stream.
   */
  public synchronized void register(final T identifier, final Stateful<IN> upstream) {
    if (closed) {
      return;
    }

    Disposable.Swap previous = subscriptions.remove(identifier);
    if (previous != null) {
      previous.dispose();
    }

    // Track the registration before subscribing, because the subscription
    // might signal synchronously (on this thread) before `subscribe` returns.
    final Disposable.Swap registration = Disposables.swap();
    subscriptions.put(identifier, registration);
    states.put(identifier, upstream.state());
    transition(transformer.apply(states.values()));

    registration.update(upstream.states().subscribe(
      s -> onUpstreamState(identifier, registration, s),
      e -> onUpstreamTerminated(identifier, registration),
      () -> onUpstreamTerminated(identifier, registration)
    ));
  }

  private synchronized void onUpstreamState(final T identifier, final Disposable.Swap registration, final IN state) {
    if (subscriptions.get(identifier) != registration) {
      return; // deregistered (or registered again) while this signal was in flight
    }
    states.put(identifier, state);
    transition(transformer.apply(states.values()));
  }

  private synchronized void onUpstreamTerminated(final T identifier, final Disposable.Swap registration) {
    if (subscriptions.get(identifier) != registration) {
      return; // deregistered (or registered again) while this signal was in flight
    }
    subscriptions.remove(identifier);
    states.remove(identifier);
    transitionAfterRemoval();
  }

  /**
   * Deregisters a stateful element from the composite.
   *
   * <p>Note that it might be possible that the passed in identifier is already deregistered (for example if
   * the upstream flux already completed or failed). In this case, this is essentially a "noop" since the target state,
   * the identifier not being part of the stateful component, is already reached.</p>
   *
   * @param identifier the unique identifier to use.
   */
  public synchronized void deregister(final T identifier) {
    if (closed) {
      return;
    }

    Disposable.Swap registration = subscriptions.remove(identifier);
    if (registration != null) {
      registration.dispose();
      states.remove(identifier);
    }
    transitionAfterRemoval();
  }

  private void transitionAfterRemoval() {
    transition(subscriptions.isEmpty() ? initialState : transformer.apply(states.values()));
  }

  /**
   * Caller must hold this object's monitor.
   */
  private void transition(final OUT newState) {
    inner.transition(newState);
  }

  /**
   * Closes the composite permanently and deregisters all elements.
   * <p>
   * Transitions to the initial state (if not already in it), then completes the {@link #states()} stream.
   * Subsequent calls to {@link #register} and {@link #deregister} have no effect.
   */
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;

    subscriptions.values().forEach(Disposable::dispose);
    subscriptions.clear();
    states.clear();
    transition(initialState);
    inner.close();
  }

  @Override
  public OUT state() {
    return inner.state();
  }

  @Override
  public Flux<OUT> states() {
    return inner.states();
  }

}
