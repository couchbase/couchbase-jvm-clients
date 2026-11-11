/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public abstract class CoreModelCredential {

  private CoreModelCredential() {
  }

  public static final class Capella extends CoreModelCredential {
    private final String email;
    private final String password;
    private final String organizationId;

    public Capella(String email, String password, String organizationId) {
      this.email = requireNonNull(email);
      this.password = requireNonNull(password);
      this.organizationId = requireNonNull(organizationId);
    }

    public String email() {
      return email;
    }

    public String password() {
      return password;
    }

    public String organizationId() {
      return organizationId;
    }

    @Override
    public String toString() {
      return "Capella{<redacted>}";
    }
  }

  public static final class CredentialStore extends CoreModelCredential {
    private final String credentialName;

    public CredentialStore(String credentialName) {
      this.credentialName = requireNonNull(credentialName);
    }

    public String credentialName() {
      return credentialName;
    }

    @Override
    public String toString() {
      return "CredentialStore{<redacted>}";
    }
  }

  public static final class ApiKey extends CoreModelCredential {
    private final String key;

    public ApiKey(String key) {
      this.key = requireNonNull(key);
    }

    public String key() {
      return key;
    }

    @Override
    public String toString() {
      return "ApiKey{<redacted>}";
    }
  }
}
