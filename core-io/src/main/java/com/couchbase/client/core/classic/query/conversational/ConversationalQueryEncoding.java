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

package com.couchbase.client.core.classic.query.conversational;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreAskOptions;
import com.couchbase.client.core.api.query.CoreConversationalQueryOptions;
import com.couchbase.client.core.api.query.CoreConversationalQueryOutput;
import com.couchbase.client.core.api.query.CoreModelCredential;
import com.couchbase.client.core.api.query.CoreModelOptions;
import com.couchbase.client.core.api.query.CorePauseOptions;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryProfile;
import com.couchbase.client.core.api.query.CoreQueryScanConsistency;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.classic.query.ClassicCoreQueryOps;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

@Stability.Internal
class ConversationalQueryEncoding {
  private ConversationalQueryEncoding() {
  }

  /**
   * USING AI WITH {"keyspaces":["`travel-sample`.`inventory`.`hotel`"],"knowledge":true,"execute":false,"output":"SQL"} Find hotels in Paris
   * <p>
   * `WITH` is always included, even if empty, so that the query service does not parse the prompt as options.
   */
  static String usingAiStatement(String prompt,
                                 @Nullable List<CoreKeyspace> keyspaces,
                                 @Nullable Boolean useKnowledge,
                                 @Nullable String hint,
                                 @Nullable Boolean execute,
                                 @Nullable CoreConversationalQueryOutput output) {
    ObjectNode with = Mapper.createObjectNode();
    if (keyspaces != null) {
      ArrayNode array = with.putArray("keyspaces");
      keyspaces.forEach(ks -> array.add(keyspacePath(ks)));
    }
    if (useKnowledge != null) {
      with.put("knowledge", useKnowledge);
    }
    if (hint != null && !hint.isEmpty()) {
      with.put("hint", hint);
    }
    if (execute != null) {
      with.put("execute", execute);
    }
    if (output != null) {
      with.put("output", output.wireName());
    }
    return "USING AI WITH " + with + " " + prompt;
  }

  /**
   * BEGIN CHAT WITH {"keyspaces":["`travel-sample`.`inventory`.`hotel`"],"knowledge":false,"timeout":600}
   */
  static String beginChatStatement(List<CoreKeyspace> keyspaces,
                                   @Nullable Boolean useKnowledge,
                                   @Nullable Duration inactivityTimeout) {
    ObjectNode with = Mapper.createObjectNode();
    ArrayNode array = with.putArray("keyspaces");
    keyspaces.forEach(ks -> array.add(keyspacePath(ks)));
    // The RFC requires always sending knowledge on BEGIN, defaulting to false.
    with.put("knowledge", Boolean.TRUE.equals(useKnowledge));
    if (inactivityTimeout != null) {
      with.put("timeout", inactivityTimeout.getSeconds());
    }
    return "BEGIN CHAT WITH " + with;
  }

  /**
   * PAUSE CHAT WITH {"summarize":false}
   */
  static String pauseChatStatement(@Nullable Boolean summarize) {
    if (summarize == null) {
      return "PAUSE CHAT";
    }
    ObjectNode with = Mapper.createObjectNode();
    with.put("summarize", summarize);
    return "PAUSE CHAT WITH " + with;
  }

  static ObjectNode statementBody(String statement) {
    return Mapper.createObjectNode().put("statement", statement);
  }

  static ObjectNode queryBody(String prompt, List<CoreKeyspace> keyspaces, CoreConversationalQueryOptions options) {
    validateRawParameters(options.raw());
    ObjectNode body = ClassicCoreQueryOps.convertOptions(asQueryOptions(options));
    // The RFC requires always sending knowledge on a one-shot query, defaulting to false.
    body.put("statement", usingAiStatement(prompt, keyspaces, Boolean.TRUE.equals(options.useKnowledge()),
        options.hint(), options.execute(), options.output()));
    applyModelInputs(body, options.model(), options.credential());
    return body;
  }

  static ObjectNode askBody(String prompt, CoreAskOptions options) {
    validateRawParameters(options.raw());
    ObjectNode body = ClassicCoreQueryOps.convertOptions(asQueryOptions(options));
    body.put("statement", usingAiStatement(prompt, null, null, options.hint(), options.execute(), options.output()));
    applyModelInputs(body, options.model(), options.credential());
    return body;
  }

  static ObjectNode pauseBody(CorePauseOptions options) {
    ObjectNode body = statementBody(pauseChatStatement(options.summarize()));
    if (!Boolean.FALSE.equals(options.summarize())) {
      applyModelInputs(body, options.model(), options.credential());
    }
    return body;
  }

  static final String RESUME_CHAT_STATEMENT = "RESUME CHAT";

  static final String END_CHAT_STATEMENT = "END CHAT";

  static String keyspacePath(CoreKeyspace keyspace) {
    return "`" + keyspace.bucket() + "`.`" + keyspace.scope() + "`.`" + keyspace.collection() + "`";
  }

  /**
   * Allows reusing {@link ClassicCoreQueryOps#convertOptions}, so the ordinary query options retain their existing
   * wire mappings.
   */
  static CoreQueryOptions asQueryOptions(CoreAskOptions opts) {
    return new CoreQueryOptions() {
      @Override
      public boolean adhoc() {
        return true;
      }

      @Override
      public @Nullable String clientContextId() {
        return opts.clientContextId();
      }

      @Override
      public @Nullable CoreMutationState consistentWith() {
        return opts.consistentWith();
      }

      @Override
      public @Nullable Integer maxParallelism() {
        return null;
      }

      @Override
      public boolean metrics() {
        return Boolean.TRUE.equals(opts.metrics());
      }

      @Override
      public @Nullable ObjectNode namedParameters() {
        return null;
      }

      @Override
      public @Nullable Integer pipelineBatch() {
        return null;
      }

      @Override
      public @Nullable Integer pipelineCap() {
        return null;
      }

      @Override
      public @Nullable ArrayNode positionalParameters() {
        return null;
      }

      @Override
      public @Nullable CoreQueryProfile profile() {
        return opts.profile();
      }

      @Override
      public @Nullable JsonNode raw() {
        return opts.raw();
      }

      @Override
      public boolean readonly() {
        return false;
      }

      @Override
      public @Nullable Duration scanWait() {
        return opts.scanWait();
      }

      @Override
      public @Nullable Integer scanCap() {
        return null;
      }

      @Override
      public @Nullable CoreQueryScanConsistency scanConsistency() {
        return opts.scanConsistency();
      }

      @Override
      public boolean flexIndex() {
        return false;
      }

      @Override
      public @Nullable Boolean preserveExpiry() {
        return null;
      }

      @Override
      public @Nullable CoreSingleQueryTransactionOptions asTransactionOptions() {
        return null;
      }

      @Override
      public CoreCommonOptions commonOptions() {
        return opts.commonOptions();
      }

      @Override
      public @Nullable Boolean useReplica() {
        return opts.useReplica();
      }
    };
  }

  static void validateRawParameters(@Nullable JsonNode raw) {
    if (raw == null) {
      return;
    }
    for (Iterator<String> it = raw.fieldNames(); it.hasNext(); ) {
      String name = it.next();
      if (name.toLowerCase(Locale.ROOT).startsWith("natural")) {
        // Required by RFC
        throw InvalidArgumentException.fromMessage("Raw parameters cannot begin with 'natural', as these are reserved for conversational query: " + name);
      }
    }
  }

  static void validateModelInputs(@Nullable CoreModelOptions model, @Nullable CoreModelCredential credential) {
    // RFC requires rejecting these params if they are sent in raw.
    if (model != null && model.raw() != null) {
      for (Iterator<String> it = model.raw().fieldNames(); it.hasNext(); ) {
        String name = it.next();
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.equals("cred_id") || lower.equals("api_key")) {
          // Required by RFC
          throw InvalidArgumentException.fromMessage("Model credentials must be provided with a model credential, not in raw model options: " + name);
        }
      }
    }

    if (credential instanceof CoreModelCredential.Capella && model != null) {
      List<String> unsupported = new ArrayList<>();
      if (model.endpoint() != null) unsupported.add("endpoint");
      if (model.region() != null) unsupported.add("region");
      if (model.outputTokenLimit() != null) unsupported.add("outputTokenLimit");
      if (model.moderation() != null) unsupported.add("moderation");
      if (model.raw() != null) unsupported.add("raw");
      if (!unsupported.isEmpty()) {
        throw InvalidArgumentException.fromMessage("These model options are not supported with a Capella model credential: " + unsupported);
      }
    }
  }

  /**
   * The Capella path is used if the credential is {@link CoreModelCredential.Capella}, else the direct-provider path.
   */
  static void applyModelInputs(ObjectNode body, @Nullable CoreModelOptions model, @Nullable CoreModelCredential credential) {
    validateModelInputs(model, credential);

    if (credential instanceof CoreModelCredential.Capella) {
      CoreModelCredential.Capella capella = (CoreModelCredential.Capella) credential;

      if (model != null) {
        if (model.provider() != null) {
          body.put("natural_vendor", model.provider());
        }
        if (model.name() != null) {
          body.put("natural_model", model.name());
        }
      }

      body.put("natural_cred", capella.email() + ":" + capella.password());
      body.put("natural_orgid", capella.organizationId());
    } else {
      ObjectNode config = Mapper.createObjectNode();

      if (model != null) {
        // Raw first, so any typed options that are set take precedence.
        if (model.raw() != null) {
          config.setAll(model.raw());
        }
        if (model.provider() != null) {
          config.put("provider", model.provider());
        }
        if (model.name() != null) {
          config.put("model", model.name());
        }
        if (model.endpoint() != null) {
          config.put("endpoint", model.endpoint());
        }
        if (model.region() != null) {
          config.put("region", model.region());
        }
        if (model.outputTokenLimit() != null) {
          config.put("output_token_limit", model.outputTokenLimit());
        }
        if (model.moderation() != null) {
          config.put("moderation", model.moderation());
        }
      }

      if (credential instanceof CoreModelCredential.CredentialStore) {
        config.put("cred_id", ((CoreModelCredential.CredentialStore) credential).credentialName());
      } else if (credential instanceof CoreModelCredential.ApiKey) {
        config.put("api_key", ((CoreModelCredential.ApiKey) credential).key());
      }

      if (!config.isEmpty()) {
        body.set("natural_config", config);
      }
    }
  }
}
