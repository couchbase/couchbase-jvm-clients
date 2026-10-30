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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.query.CoreAskOptions;
import com.couchbase.client.core.api.query.CoreBeginChatOptions;
import com.couchbase.client.core.api.query.CoreBeginChatResult;
import com.couchbase.client.core.api.query.CoreChatState;
import com.couchbase.client.core.api.query.CoreConversationalQueryMetaData;
import com.couchbase.client.core.api.query.CoreConversationalQueryOps;
import com.couchbase.client.core.api.query.CoreConversationalQueryOptions;
import com.couchbase.client.core.api.query.CoreConversationalQueryResult;
import com.couchbase.client.core.api.query.CorePauseOptions;
import com.couchbase.client.core.api.query.CoreTokenUsage;
import com.couchbase.client.core.classic.query.ClassicCoreQueryMetaData;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.query.ConversationalQueryRequest;
import com.couchbase.client.core.msg.query.ConversationalQueryTrailer;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.topology.HostAndServicePorts;
import com.couchbase.client.core.util.Golang;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.couchbase.client.core.classic.query.conversational.ConversationalQueryRequests.request;
import static com.couchbase.client.core.classic.query.conversational.ConversationalQueryRequests.timeout;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

/*
 * Nb this does duplicate some logic from ClassicCoreQueryOps, as the types are very different
 * and much simpler support is needed here; no transactions or error conversion, for example.
 * Though the types could be handled with templates, I'm aiming to de-risk this feature by avoiding
 * major changes to the core query path.
 */
@Stability.Internal
public class ClassicCoreConversationalQueryOps implements CoreConversationalQueryOps {
  private static final String OPERATION_ASK = "ask";
  private static final String OPERATION_PAUSE = "pause";
  private static final String OPERATION_END = "end";

  private final Core core;

  public ClassicCoreConversationalQueryOps(Core core) {
    this.core = requireNonNull(core);
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryResult> queryAsync(String prompt,
                                                                    List<CoreKeyspace> keyspaces,
                                                                    CoreConversationalQueryOptions options) {
    return toResponse(Mono.defer(() -> {
      notNullOrEmpty(prompt, "Prompt");
      notNull(keyspaces, "Keyspaces");
      notNull(options, "Options");
      if (keyspaces.isEmpty()) {
        throw InvalidArgumentException.fromMessage("At least one keyspace (collection) must be provided");
      }

      ObjectNode body = ConversationalQueryEncoding.queryBody(prompt, keyspaces, options);
      return execute(request(core, body, timeout(core, options.commonOptions()), options.commonOptions(),
          TracingIdentifiers.SPAN_REQUEST_CONVERSATIONAL_QUERY, true, null, null));
    }).flatMap(ClassicCoreConversationalQueryOps::conversationalQueryResult));
  }

  @Override
  public CoreAsyncResponse<CoreBeginChatResult> beginChatAsync(List<CoreKeyspace> keyspaces, CoreBeginChatOptions options) {
    return toResponse(Mono.defer(() -> {
      notNull(keyspaces, "Keyspaces");
      notNull(options, "Options");
      if (keyspaces.isEmpty()) {
        throw InvalidArgumentException.fromMessage("At least one keyspace (collection) must be provided");
      }
      // The model inputs are not sent with BEGIN, but the RFC requires validating them before it.
      ConversationalQueryEncoding.validateModelInputs(options.model(), options.credential());

      Duration timeout = timeout(core, options.commonOptions());
      String statement = ConversationalQueryEncoding.beginChatStatement(keyspaces, options.useKnowledge(), options.inactivityTimeout());

      return core.waitForClusterTopology(timeout)
          .map(ConversationalQueryRouting::selectChatNode)
          .flatMap(node -> execute(request(core, ConversationalQueryEncoding.statementBody(statement), timeout,
                  options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_CONVERSATIONAL_QUERY_BEGIN_CHAT, false, null, node.id()))
              .flatMap(response -> response.rows().then(response.trailer()))
              .map(trailer -> {
                String chatId = trailer.conversational()
                    .flatMap(ConversationalQueryTrailer::chatId)
                    .orElseThrow(() -> new CouchbaseException("The query service did not return a chat id from BEGIN CHAT"));
                return new CoreBeginChatResult(chatId, requireNonNull(node.uuid()));
              }));
    }));
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryResult> askAsync(CoreChatState chat, String prompt, CoreAskOptions options) {
    return toResponse(Mono.defer(() -> {
      notNullOrEmpty(prompt, "Prompt");
      notNull(options, "Options");

      return sendToOwner(chat, OPERATION_ASK, ConversationalQueryEncoding.askBody(prompt, options), options.commonOptions(),
          TracingIdentifiers.SPAN_REQUEST_CONVERSATIONAL_QUERY_ASK,
          ClassicCoreConversationalQueryOps::conversationalQueryResult);
    }));
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryMetaData> pauseAsync(CoreChatState chat, CorePauseOptions options) {
    return toResponse(Mono.defer(() -> {
      notNull(options, "Options");

      return sendToOwner(chat, OPERATION_PAUSE, ConversationalQueryEncoding.pauseBody(options), options.commonOptions(),
          TracingIdentifiers.SPAN_REQUEST_CONVERSATIONAL_QUERY_PAUSE,
          ClassicCoreConversationalQueryOps::conversationalMetaData);
    }).doOnNext(ignored -> chat.owner(null)));
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryMetaData> resumeAsync(CoreChatState chat, CoreCommonOptions options) {
    return toResponse(Mono.defer(() -> {
      checkChat(chat);
      notNull(options, "Options");
      Duration timeout = timeout(core, options);

      return core.waitForClusterTopology(timeout)
          .flatMap(topology -> {
            String retained = chat.owner();
            HostAndServicePorts node = ConversationalQueryRouting.resolveOwnerOrSelectChatNode(topology, retained);
            String nodeUuid = requireNonNull(node.uuid());
            boolean toRetainedOwner = nodeUuid.equals(retained);

            ConversationalQueryRequest request = request(core,
                ConversationalQueryEncoding.statementBody(ConversationalQueryEncoding.RESUME_CHAT_STATEMENT),
                timeout, options, TracingIdentifiers.SPAN_REQUEST_CONVERSATIONAL_QUERY_RESUME, false, chat.chatId(), node.id());

            return execute(request)
                .flatMap(ClassicCoreConversationalQueryOps::conversationalMetaData)
                // The owner must be recorded before the RESUME completes.
                .doOnNext(ignored -> chat.owner(nodeUuid));
          });
    }));
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryMetaData> endAsync(CoreChatState chat, CoreCommonOptions options) {
    return toResponse(Mono.defer(() -> sendToOwner(chat, OPERATION_END,
            ConversationalQueryEncoding.statementBody(ConversationalQueryEncoding.END_CHAT_STATEMENT), options,
            TracingIdentifiers.SPAN_REQUEST_CONVERSATIONAL_QUERY_END,
            ClassicCoreConversationalQueryOps::conversationalMetaData))
        .doOnNext(ignored -> chat.close()));
  }

  // ASK, PAUSE and END must go to the retained owner.
  // This throws on invalid arguments, so should be called inside Mono.defer.
  private <T> Mono<T> sendToOwner(CoreChatState chat,
                                  String operation,
                                  ObjectNode body,
                                  CoreCommonOptions common,
                                  String spanName,
                                  Function<QueryResponse, Mono<T>> handler) {
    checkChat(chat);
    notNull(common, "Options");
    Duration timeout = timeout(core, common);

    return core.waitForClusterTopology(timeout)
        .flatMap(topology -> {
          String owner = chat.owner();
          HostAndServicePorts node = ConversationalQueryRouting.resolveOwner(topology, chat.chatId(), owner, operation);
          return execute(request(core, body, timeout, common, spanName, false, chat.chatId(), node.id()))
              .flatMap(handler);
        });
  }

  private static void checkChat(CoreChatState chat) {
    notNull(chat, "Chat");
    if (chat.closed()) {
      throw InvalidArgumentException.fromMessage("This chat has been ended, so cannot be used further");
    }
  }

  private static <T> CoreAsyncResponse<T> toResponse(Mono<T> mono) {
    return new CoreAsyncResponse<>(mono.onErrorMap(ConversationalQueryErrors::remap).toFuture(), () -> {});
  }

  private Mono<QueryResponse> execute(ConversationalQueryRequest request) {
    return Mono.defer(() -> {
      core.send(request);
      return Reactor
          .wrap(request, request.response(), true)
          .doOnNext(ignored -> request.context().logicallyComplete())
          .doOnError(err -> request.context().logicallyComplete(err));
    });
  }

  private static Mono<CoreConversationalQueryResult> conversationalQueryResult(QueryResponse response) {
    return response
        .rows()
        // This buffers in-memory, same as the non-conversational async query API.
        .collectList()
        .flatMap(rows -> response
            .trailer()
            .map(trailer -> conversationalQueryResult(response.header(), rows, trailer)));
  }

  private static CoreConversationalQueryResult conversationalQueryResult(QueryChunkHeader header,
                                                                         List<QueryChunkRow> rows,
                                                                         QueryChunkTrailer trailer) {
    return new CoreConversationalQueryResult(rows, conversationalMetaData(header, trailer),
        trailer.conversational().flatMap(ConversationalQueryTrailer::generatedStatement));
  }

  // For the chat lifecycle operations, which return only metadata.
  private static Mono<CoreConversationalQueryMetaData> conversationalMetaData(QueryResponse response) {
    return response
        .rows()
        .then(response.trailer())
        .map(trailer -> conversationalMetaData(response.header(), trailer));
  }

  private static CoreConversationalQueryMetaData conversationalMetaData(QueryChunkHeader header,
                                                                        QueryChunkTrailer trailer) {
    Optional<Duration> naturalLanguageProcessingTime = Optional.empty();
    Optional<Boolean> generatedStatementExecuted = Optional.empty();
    if (trailer.metrics().isPresent()) {
      JsonNode metrics = Mapper.decodeIntoTree(trailer.metrics().get());
      JsonNode nlpTime = metrics.get("naturalLanguageProcessingTime");
      if (nlpTime != null && nlpTime.isTextual()) {
        try {
          naturalLanguageProcessingTime = Optional.of(Golang.parseDuration(nlpTime.asText()));
        } catch (InvalidArgumentException e) {
          // Skipped rather than failing an operation whose statement may already have executed.
        }
      }
      JsonNode executed = metrics.get("generatedStatementExecuted");
      if (executed != null && executed.isBoolean()) {
        generatedStatementExecuted = Optional.of(executed.booleanValue());
      }
    }

    Optional<ConversationalQueryTrailer> conversational = trailer.conversational();

    return new CoreConversationalQueryMetaData(
        ClassicCoreQueryMetaData.from(header, trailer),
        conversational.flatMap(ConversationalQueryTrailer::requestTokens).map(CoreTokenUsage::fromJson),
        conversational.flatMap(ConversationalQueryTrailer::chatTokens).map(CoreTokenUsage::fromJson),
        naturalLanguageProcessingTime,
        generatedStatementExecuted
    );
  }
}
