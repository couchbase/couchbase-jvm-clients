/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.manager.search;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;

import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;

@Stability.Internal
public class ClassicCoreScopeSearchIndexManager extends ClassicCoreBaseSearchIndexManager {

  private final CoreBucketAndScope scope;

  @Stability.Internal
  public ClassicCoreScopeSearchIndexManager(Core core, CoreBucketAndScope scope) {
    super(core);
    this.scope = scope;
  }

  @Override
  String indexesPath() {
    return CoreHttpPath.formatPath(
            "/api/bucket/{}/scope/{}/index",
            scope.bucketName(),
            scope.scopeName()
    );
  }

  @Override
  String indexPath(String indexName) {
    return indexesPath() + "/" + urlEncode(indexName);
  }
}
