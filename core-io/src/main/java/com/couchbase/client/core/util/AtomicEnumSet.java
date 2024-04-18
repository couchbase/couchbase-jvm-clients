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

package com.couchbase.client.core.util;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.core.util.CbCollections.listCopyOf;
import static java.util.Objects.requireNonNull;

/**
 * A thread-safe cousin of {@link EnumSet} that performs well in use cases
 * dominated by reads.
 * <p>
 * In high-concurrency, mutation-heavy use cases, a standard {@code EnumSet}
 * wrapped by {@link Collections#synchronizedSet(Set)} might perform better.
 */
public class AtomicEnumSet<E extends Enum<E>> extends AbstractSet<E> {
  private final AtomicLong bits = new AtomicLong();
  private final Class<E> enumClass;
  private final List<E> allValues;

  private AtomicEnumSet(Class<E> enumClass) {
    this.enumClass = requireNonNull(enumClass);
    this.allValues = listCopyOf(EnumSet.allOf(enumClass));

    long numValues = allValues.size();
    if (numValues > 64) {
      throw new IllegalArgumentException(enumClass + " has too many enum values to store in this kind of set.");
    }
  }

  public static <E extends Enum<E>> AtomicEnumSet<E> noneOf(Class<E> enumClass) {
    return new AtomicEnumSet<>(enumClass);
  }

  public static <E extends Enum<E>> AtomicEnumSet<E> allOf(Class<E> enumClass) {
    AtomicEnumSet<E> set = noneOf(enumClass);
    set.addAll(set.allValues);
    return set;
  }

  @Override
  public boolean contains(Object value) {
    try {
      @SuppressWarnings("unchecked")
      E e = (E) value;
      return (bits.get() & mask(e)) != 0;

    } catch (ClassCastException | NullPointerException e) {
      return false;
    }
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    requireNonNull(c);

    try {
      long mask = 0L;
      for (Object value : c) {
        @SuppressWarnings("unchecked")
        E e = (E) value;
        mask |= mask(e);
      }

      return (bits.get() & mask) == mask;

    } catch (ClassCastException | NullPointerException e) {
      return false;
    }
  }

  public boolean contains(E value) {
    if (value == null) {
      return false;
    }
    return (bits.get() & mask(value)) != 0;
  }

  @Override
  public boolean add(E value) {
    return setBits(mask(value));
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return setBits(mask(c, MaskMode.REJECT_NULLS));
  }

  @Override
  public boolean remove(Object value) {
    if (!enumClass.isInstance(value)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    long mask = mask((E) value);
    return clearBits(mask);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return clearBits(mask(c, MaskMode.ACCEPT_NULLS));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return clearBits(~mask(c, MaskMode.ACCEPT_NULLS));
  }

  @Override
  public Iterator<E> iterator() {
    long snapshot = bits.get();
    return allValues.stream()
      .filter(value -> (snapshot & mask(value)) != 0)
      .iterator();
  }

  @Override
  public int size() {
    return Long.bitCount(bits.get());
  }

  @Override
  public boolean isEmpty() {
    return bits.get() == 0;
  }

  @Override
  public void clear() {
    bits.set(0L);
  }

  private long mask(E value) {
    return (1L << value.ordinal());
  }

  private enum MaskMode {
    /**
     * Null values are fine in this context; they just won't be reflected in the bitmask.
     */
    ACCEPT_NULLS,

    /**
     * Null values are prohibited in this context, and should result in a NullPointerException.
     */
    REJECT_NULLS,
  }

  private long mask(Iterable<?> values, MaskMode mode) {
    long mask = 0L;
    for (Object value : values) {
      if (enumClass.isInstance(value)) {
        @SuppressWarnings("unchecked")
        E e = (E) value;
        mask |= mask(e);
      } else if (mode == MaskMode.REJECT_NULLS) {
        requireNonNull(value);
      }
    }
    return mask;
  }

  private boolean setBits(long mask) {
    long old = bits.getAndUpdate(bits -> bits | mask);
    return (old & mask) != mask;
  }

  private boolean clearBits(long mask) {
    long old = bits.getAndUpdate(bits -> bits & ~mask);
    return (old & mask) != 0;
  }

}
