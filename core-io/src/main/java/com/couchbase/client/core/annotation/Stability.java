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

package com.couchbase.client.core.annotation;

import java.lang.annotation.Documented;

/**
 * This annotation describes the stability guarantees of the annotated
 * interface, class or method.
 *
 * @since 1.0.0
 */
public @interface Stability {

  /**
   * Types/Methods/Interfaces marked as volatile can change any time and
   * for any reason.
   *
   * <p>They may be volatile for reasons including:</p>
   *
   * <ul>
   * <li>Depends on specific implementation detail within the library which
   * may change in the response.</li>
   * <li>Depends on specific implementation detail within the server which may
   * change in the response.</li>
   * <li>Has been introduced as part of a trial phase for the specific feature.</li>
   * </ul>
   */
  @Documented
  @interface Volatile {
  }

  /**
   * No commitment is made about the interface.
   *
   * <p>It may be changed in incompatible ways and dropped from one release
   * to another. The difference between an uncommitted interface and a volatile
   * interface is its maturity and likelihood of being changed. Uncommitted
   * interfaces may mature into committed interfaces.</p>
   */
  @Documented
  @interface Uncommitted {
  }

  /**
   * A committed interface is the highest grade of stability, and is the preferred attribute
   * level for consumers of the library.
   *
   * <p>Couchbase tries at best effort to preserve committed interfaces between major
   * versions, and changes to committed interfaces within a major version
   * is highly exceptional. Such exceptions may include situations where the interface
   * may lead to data corruption, security holes etc.</p>
   *
   * <p>Explicitly note that backwards-compatible extensions are always allowed since
   * they don't brake old code.</p>
   *
   * <p>This is the default interface level for an API, unless the API is specifically
   * marked otherwise.</p>
   */
  @Documented
  @interface Committed {
  }


  /**
   * This is internal API and may not be relied on at all.
   *
   * <p>Similar to volatile interfaces, no promises are made about these kinds of interfaces and
   * they change or may vanish at every point in time. But in addition to that, those interfaces
   * are explicitly marked as being designed for internal use and not for external consumption
   * in the first place.</p>
   *
   * <p>Use at your own risk!</p>
   */
  @Documented
  @interface Internal {
  }

}
