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

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Represents a single stateful component.
 */
public class SingleStateful<S> implements Stateful<S> {

  private final DirectProcessor<S> configs = DirectProcessor.create();
  private final FluxSink<S> configsSink = configs.sink();
  private final AtomicReference<S> currentState;
  private final BiConsumer<S, S> beforeTransitionCallback;


  private SingleStateful(final S initialState, final BiConsumer<S, S> beforeTransitionCallback) {
    notNull(initialState, "Initial Stateful Type");

    this.currentState = new AtomicReference<>(initialState);
    this.beforeTransitionCallback = beforeTransitionCallback;
    configsSink.next(initialState);
  }

  /**
   * Creates a new stateful component with an initial state.
   *
   * @param initialState the initial state of the component.
   * @return an initialized stateful component with the state provided.
   */
  public static <S> SingleStateful<S> fromInitial(final S initialState) {
    return fromInitial(initialState, (oldState, newState) -> {});
  }

  /**
   * Creates a new stateful component with an initial state.
   *
   * @param initialState the initial state of the component.
   * @return an initialized stateful component with the state provided.
   */
  public static <S> SingleStateful<S> fromInitial(final S initialState, final BiConsumer<S, S> beforeTransitionCallback) {
    return new SingleStateful<>(initialState, beforeTransitionCallback);
  }

  @Override
  public S state() {
    return currentState.get();
  }

  @Override
  public Flux<S> states() {
    return configs;
  }

  /**
   * Transition into a new state, notifying consumers.
   *
   * <p>Note that if the new state is identical to the old state no transition will be performed.</p>
   *
   * @param newState the new state to apply.
   */
  public synchronized void transition(final S newState) {
    notNull(newState, "New Stateful Type");

    if (!currentState.get().equals(newState)) {
      beforeTransitionCallback.accept(currentState.get(), newState);
      currentState.set(newState);
      configsSink.next(newState);
    }
  }

  /**
   * If the expected state is in place the new one is applied and consumers notified.
   *
   * @param expectedState the old expected state.
   * @param newState the new state to apply.
   * @return true if the comparison has been successful.
   */
  public boolean compareAndTransition(final S expectedState, final S newState) {
    notNull(newState, "New Stateful Type");
    notNull(expectedState, "Expected Stateful Type");

    if (currentState.compareAndSet(expectedState, newState)) {
      beforeTransitionCallback.accept(expectedState, newState);
      configsSink.next(newState);
      return true;
    }
    return false;
  }

  /**
   * Doesn't have to be called, added for good measure.
   */
  public void close() {
    configsSink.complete();
  }

}
