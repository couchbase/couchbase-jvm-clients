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
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Represents a stateful component of one or more individual stateful elements.
 */
public class CompositeStateful<T, IN, OUT> implements Stateful<OUT> {

  private final OUT initialState;
  private final Map<T, IN> states;
  private final SingleStateful<OUT> inner;
  private final Map<T, Disposable> subscriptions;
  private final BiConsumer<OUT, OUT> beforeTransitionCallback;
  private final Function<Collection<IN>, OUT> transformer;

  private CompositeStateful(final OUT initialState, final Function<Collection<IN>, OUT> transformer,
                            final BiConsumer<OUT, OUT> beforeTransitionCallback) {
    this.inner = SingleStateful.fromInitial(initialState);
    this.initialState = initialState;
    this.transformer = transformer;
    this.subscriptions = new ConcurrentHashMap<>();
    this.states = new ConcurrentHashMap<>();
    this.beforeTransitionCallback = beforeTransitionCallback;
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
   *
   * @param identifier the unique identifier to use.
   * @param upstream the upstream flux with the state stream.
   */
  public synchronized void register(final T identifier, final Stateful<IN> upstream) {
    states.put(identifier, upstream.state());
    transition(transformer.apply(states.values()));

    Disposable subscription = upstream.states().subscribe(
      s -> {
        states.put(identifier, s);
        transition(transformer.apply(states.values()));
      },
      e -> deregister(identifier),
      () -> deregister(identifier)
    );

    subscriptions.put(identifier, subscription);
  }

  /**
   * Deregisters a stateful element from the composite.
   *
   * @param identifier the unique identifier to use.
   */
  public synchronized void deregister(final T identifier) {
    Disposable subscription = subscriptions.remove(identifier);
    if (!subscription.isDisposed()) {
      subscription.dispose();
      states.remove(identifier);
      transition(transformer.apply(states.values()));
    }
    if (subscriptions.isEmpty()) {
      transition(initialState);
    }
  }

  private void transition(final OUT newState) {
    if (!inner.state().equals(newState)) {
      beforeTransitionCallback.accept(inner.state(), newState);
      inner.transition(newState);
    }
  }

  /**
   * Closes the composite permanently and deregisters all elements.
   */
  public synchronized void close() {
    Set<T> identifiers = new HashSet<>(subscriptions.keySet());
    for (T identifier : identifiers) {
      deregister(identifier);
    }
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
