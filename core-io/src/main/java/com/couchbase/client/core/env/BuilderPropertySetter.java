/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.Golang;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.mapCopyOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("rawtypes")
@Stability.Internal
public class BuilderPropertySetter {

  // By convention, builder methods that expose child builders have names ending with this.
  private final String childBuilderAccessorSuffix;

  // Escape hatch in case some accessors don't follow the convention.
  private final Map<String, String> irregularChildBuilderAccessors;

  public BuilderPropertySetter() {
    this("Config", mapOf("ioEnvironment", "ioEnvironment"));
  }

  public BuilderPropertySetter(
    String childBuilderAccessorSuffix,
    Map<String, String> irregularChildBuilderAccessors
  ) {
    this.childBuilderAccessorSuffix = requireNonNull(childBuilderAccessorSuffix);
    this.irregularChildBuilderAccessors = mapCopyOf(irregularChildBuilderAccessors);
  }

  /**
   * @throws InvalidPropertyException if any property could not be applied to the builder
   */
  public void set(Object builder, Map<String, String> properties) {
    properties.forEach((key, value) -> set(builder, key, value));
  }

  /**
   * @throws InvalidPropertyException if the property could not be applied to the builder
   */
  public void set(Object builder, String propertyName, String propertyValue) {


    try {
      final List<String> propertyComponents = Arrays.asList(propertyName.split("\\.", -1));
      final List<String> pathToBuilder = propertyComponents.subList(0, propertyComponents.size() - 1);
      final String setterName = propertyComponents.get(propertyComponents.size() - 1);

      for (String pathComponent : pathToBuilder) {
        try {
          final String childBuilderAccessor = irregularChildBuilderAccessors.getOrDefault(
            pathComponent,
            pathComponent + childBuilderAccessorSuffix
          );

          AtomicReference<Object> ref = new AtomicReference<>();
          builder.getClass()
            .getMethod(childBuilderAccessor, Consumer.class)
            .invoke(builder, (Consumer<Object>) ref::set);
          builder = ref.get();

        } catch (NoSuchMethodException e) {
          throw InvalidArgumentException.fromMessage("Method not found: " + e.getMessage(), e);
        }
      }

      final List<Method> candidates = Arrays.stream(builder.getClass().getMethods())
          .filter(m -> m.getName().equals(setterName))
          .filter(m -> m.getParameterCount() == 1)
          .collect(Collectors.toList());

      if (candidates.isEmpty()) {
        throw InvalidArgumentException.fromMessage("No one-arg setter for property \"" + propertyName + "\" in " + builder.getClass());
      }

      int remainingCandidates = candidates.size();
      final List<Throwable> failedCandidates = new ArrayList<>();
      for (Method setter : candidates) {
        try {
          final Object convertedValue = typeRegistry.convert(propertyValue, setter.getGenericParameterTypes()[0]);
          setter.invoke(builder, convertedValue);
        } catch (Throwable t) {
          if (candidates.size() == 1) {
            throw t;
          }
          failedCandidates.add(t);
          if (--remainingCandidates == 0) {
            final InvalidArgumentException e = InvalidArgumentException.fromMessage(
                "Found multiple one-arg setters for property \"" + propertyName + "\" in "
                    + builder.getClass() + " but none accepted the value \"" + propertyValue + "\".");
            failedCandidates.forEach(e::addSuppressed);
            throw e;
          }
        }
      }

    } catch (InvocationTargetException e) {
      throw InvalidPropertyException.forProperty(propertyName, propertyValue, e.getCause());

    } catch (Exception e) {
      throw InvalidPropertyException.forProperty(propertyName, propertyValue, e);
    }
  }

  public static class TypeConverterRegistry {
    private final Map<Type, TypeConverter> converters = new HashMap<>();

    public TypeConverterRegistry register(Type t, TypeConverter converter) {
      converters.put(requireNonNull(t), requireNonNull(converter));
      return this;
    }

    public TypeConverterRegistry register(Type t, String expectation, Function<String, ?> conversion) {
      requireNonNull(expectation);
      requireNonNull(conversion);

      return register(t, new TypeConverter() {
        @Override
        public String expectation(Type t, TypeConverterRegistry registry) {
          return expectation;
        }

        @Override
        public Object convert(String s, TypeConverterRegistry registry, Type t) {
          return conversion.apply(s);
        }
      });
    }

    public Object convert(String value, Type targetType) {
      final TypeConverter converter = converters.get(getRawType(targetType));

      if (converter != null) {
        try {
          return converter.convert(value, this, targetType);

        } catch (Exception e) {
          throw InvalidArgumentException.fromMessage(
              "Expected " + converter.expectation(targetType, this) + " but got \"" + value + "\".", e);
        }
      }

      final Optional<Class> arrayComponentType = getArrayComponentType(targetType);
      if (arrayComponentType.isPresent()) {
        return convertArray(arrayComponentType.get(), value);
      }

      // Maybe it's an enum. We know how to convert those.
      Optional<Class> enumClass = asEnumClass(targetType);
      if (enumClass.isPresent()) {
        return convertEnum(enumClass.get(), value);
      }

      // Maybe it has a `valueOf` factory method.
      if (targetType instanceof Class) {
        try {
          final Method factoryMethod = ((Class<?>) targetType).getMethod("valueOf", String.class);
          final int modifiers = factoryMethod.getModifiers();
          if (Modifier.isStatic(modifiers)
              && Modifier.isPublic(modifiers)
              && Arrays.equals(factoryMethod.getParameterTypes(), new Class[]{String.class})) {
            return factoryMethod.invoke(null, value);
          }
        } catch (NoSuchMethodException e) {
          // oh well.
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      // Maybe Jackson can convert it for us.
      try {
        return Mapper.reader().forType(asTypeReference(targetType)).readValue(value);

      } catch (IOException e) {
        throw InvalidArgumentException.fromMessage("Expected a value Jackson can bind to " + targetType + " but got \"" + value + "\".", e);
      }
    }

    private Object convertArray(Class elementClass, String value) {
      try {
        List<String> items = splitList(value);
        Object array = Array.newInstance(elementClass, items.size());
        int i = 0;
        for (String item : items) {
          Array.set(array, i++, convert(item, elementClass));
        }
        return array;

      } catch (IllegalArgumentException e) {
        throw InvalidArgumentException.fromMessage("Expected a comma-delimited list where each item is " + expectation(elementClass) + " but got \"" + value + "\".");
      }
    }

    public String expectation(Type type) {
      final TypeConverter converter = converters.get(type);
      if (converter != null) {
        return converter.expectation(type, this);
      }

      final Optional<Class> enumClass = asEnumClass(type);
      if (enumClass.isPresent()) {
        return "one of " + Arrays.asList(enumClass.get().getEnumConstants());
      }

      throw InvalidArgumentException.fromMessage("No converter for " + type);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Enum> E convertEnum(Class<E> enumClass, String value) {
    try {
      return (E) Enum.valueOf(enumClass, value);
    } catch (IllegalArgumentException e) {
      List<String> enumValueNames = Arrays.stream(enumClass.getEnumConstants()).map(Enum::name).collect(Collectors.toList());
      throw InvalidArgumentException.fromMessage("Expected one of " + enumValueNames + " but got \"" + value + "\"");
    }
  }

  private static Class getRawType(Type t) {
    return t instanceof Class ? (Class) t : (Class) ((ParameterizedType) t).getRawType();
  }

  interface TypeConverter {
    String expectation(Type type, TypeConverterRegistry registry);

    Object convert(String value, TypeConverterRegistry registry, Type type);

    default TypeConverter simple(String expectation, Function<String, ?> conversion) {
      requireNonNull(expectation);
      requireNonNull(conversion);

      return new TypeConverter() {
        @Override
        public String expectation(Type type, TypeConverterRegistry registry) {
          return expectation;
        }

        @Override
        public Object convert(String s, TypeConverterRegistry registry, Type t) {
          return conversion.apply(s);
        }
      };
    }
  }

  private final TypeConverterRegistry typeRegistry = new TypeConverterRegistry();

  {
    typeRegistry
        .register(String.class, "a string", Function.identity())
        .register(Integer.class, "an int", Integer::parseInt)
        .register(Integer.TYPE, "an int", Integer::parseInt)
        .register(Long.class, "a long", Long::parseLong)
        .register(Long.TYPE, "a long", Long::parseLong)
        .register(Double.class, "a double", Double::parseDouble)
        .register(Double.TYPE, "a double", Double::parseDouble)
        .register(Float.class, "a float", Float::parseFloat)
        .register(Float.TYPE, "a float", Float::parseFloat)
        .register(Boolean.class, "a boolean (\"true\", \"false\", \"1\", or \"0\")", BuilderPropertySetter::parseBooleanStrict)
        .register(Boolean.TYPE, "a boolean (\"true\", \"false\", \"1\", or \"0\")", BuilderPropertySetter::parseBooleanStrict)

        .register(Duration.class, "a duration qualified by a time unit (like \"2.5s\" or \"300ms\")",
            d -> requireNonNegative(Golang.parseDuration(d)))

        .register(Path.class, "an open file from a path", Paths::get)

        .register(Iterable.class, new CollectionConverter(ArrayList.class))
        .register(Collection.class, new CollectionConverter(ArrayList.class))
        .register(List.class, new CollectionConverter(ArrayList.class))
        .register(Set.class, new CollectionConverter(LinkedHashSet.class))
    ;
  }

  private static class CollectionConverter implements TypeConverter {
    private final Class<? extends Collection> collectionClass;

    public <T extends Collection> CollectionConverter(Class<T> collectionClass) {
      this.collectionClass = requireNonNull(collectionClass);
      newCollection(); // fail fast
    }

    @Override
    public String expectation(Type type, TypeConverterRegistry registry) {
      Type itemType = ((ParameterizedType) type).getActualTypeArguments()[0];
      return "a comma-delimited list where each item is " + registry.expectation(itemType);
    }

    @Override
    public Object convert(String s, TypeConverterRegistry registry, Type t) {
      final Type itemType = ((ParameterizedType) t).getActualTypeArguments()[0];
      final Collection<Object> result = newCollection();
      for (String item : splitList(s)) {
        result.add(registry.convert(item, itemType));
      }
      return result;
    }

    @SuppressWarnings("unchecked")
    private Collection<Object> newCollection() {
      try {
        return collectionClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static Duration requireNonNegative(Duration d) {
    if (d.isNegative()) {
      throw InvalidArgumentException.fromMessage("Duration must be non-negative but got " + d);
    }
    return d;
  }

  private static boolean parseBooleanStrict(String value) {
    switch (value) {
      case "true":
      case "1":
        return true;
      case "false":
      case "0":
        return false;
      default:
        throw InvalidArgumentException.fromMessage("Expected 'true', 'false', '0', or '1' but got '" + value + "'");
    }
  }

  private static TypeReference asTypeReference(Type t) {
    requireNonNull(t);

    return new TypeReference<Void>() {
      public Type getType() {
        return t;
      }
    };
  }

  private static Optional<Class> asEnumClass(Type t) {
    return t instanceof Class && ((Class) t).isEnum()
        ? Optional.of((Class) t)
        : Optional.empty();
  }

  private static Optional<Class> getArrayComponentType(Type t) {
    return t instanceof Class && ((Class) t).isArray()
        ? Optional.of(((Class) t).getComponentType())
        : Optional.empty();
  }

  private static List<String> splitList(String commaDelimitedList) {
    final List<String> result = new ArrayList<>();
    for (String s : commaDelimitedList.split(",")) {
      final String trimmed = s.trim();
      if (!trimmed.isEmpty()) {
        result.add(trimmed);
      }
    }
    return result;
  }
}
