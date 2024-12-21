/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.HashSet;
import java.util.Set;

@Stability.Internal
public class CbAnnotations {
  private CbAnnotations() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Searches the element for an annotation of the given class and returns the first match.
   * This is a recursive search that also considers meta-annotations.
   *
   * @param element the element to search
   * @param annotationClass the type of annotation to look for
   * @return Matching annotation, or null if not found.
   */
  public static <T extends Annotation> @Nullable T findAnnotation(AnnotatedElement element, Class<T> annotationClass) {
    for (Annotation a : element.getAnnotations()) {
      T meta = findAnnotation(a, annotationClass);
      if (meta != null) {
        return meta;
      }
    }
    return null;
  }

  /**
   * Searches the given annotation and any meta-annotations, returning the first annotation of the given class.
   *
   * @param annotation the root annotation to search
   * @param annotationClass the type of annotation to look for
   * @return Matching annotation, or null if not found.
   */
  public static <T extends Annotation> @Nullable T findAnnotation(Annotation annotation, Class<T> annotationClass) {
    return findAnnotationRecursive(annotation, annotationClass, new HashSet<>());
  }

  private static <T extends Annotation> @Nullable T findAnnotationRecursive(Annotation annotation, Class<T> annotationClass, Set<Class<?>> seen) {
    final Class<?> c = annotation.annotationType();

    // Annotations can be annotated with themselves (@Documented is common example)
    // in which case we need to bail out to avoid stack overflow.
    if (!seen.add(c)) {
      return null;
    }

    if (c.equals(annotationClass)) {
      return annotationClass.cast(annotation);
    }

    for (Annotation meta : c.getAnnotations()) {
      T found = findAnnotationRecursive(meta, annotationClass, seen);
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
