/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.util.MeteringUnits;
import com.couchbase.client.core.transaction.util.TransactionKVHandler;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.util.CbStrings;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.couchbase.client.core.transaction.support.OptionsUtil.createClientContext;
import static com.couchbase.client.core.transaction.support.OptionsUtil.kvTimeoutNonMutating;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_ATTEMPTS;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_DOCS_INSERTED;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_DOCS_REMOVED;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_DOCS_REPLACED;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_DURABILITY_LEVEL;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_EXPIRES_AFTER_MILLIS;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_FORWARD_COMPATIBILITY;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_START_COMMIT;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_START_TIMESTAMP;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_STATUS;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_TIMESTAMP_COMPLETE;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_TIMESTAMP_ROLLBACK_START;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_TRANSACTION_ID;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Stability.Internal
public class ActiveTransactionRecord {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ActiveTransactionRecord() {
    }

    // Called from FIT
    public static Mono<Optional<ActiveTransactionRecordEntry>> findEntryForTransaction(Core core,
                                                                                     CollectionIdentifier atrCollection,
                                                                                     String atrId,
                                                                                     String attemptId,
                                                                                     CoreMergedTransactionConfig config,
                                                                                     @Nullable SpanWrapper pspan,
                                                                                     @Nullable CoreTransactionLogger logger) {
      return findEntryForTransaction(core, atrCollection, atrId, attemptId, config, pspan, logger, null);
    }

    public static Mono<Optional<ActiveTransactionRecordEntry>> findEntryForTransaction(Core core,
                                                                                      CollectionIdentifier atrCollection,
                                                                                      String atrId,
                                                                                      String attemptId,
                                                                                      CoreMergedTransactionConfig config,
                                                                                      @Nullable SpanWrapper pspan,
                                                                                      @Nullable CoreTransactionLogger logger,
                                                                                      @Nullable MeteringUnits.MeteringUnitsBuilder units) {

        return TransactionKVHandler.lookupIn(core, atrCollection, atrId, kvTimeoutNonMutating(core),
                        false, createClientContext("ATR::findEntryForTransaction"), pspan,
                        false,
                        Arrays.asList(
                                new SubdocGetRequest.Command(SubdocCommandType.GET, ATR_FIELD_ATTEMPTS + "." + attemptId, true, 0),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "$vbucket.HLC", true, 1)
                        ))

                .map(d -> {
                    if (units != null) {
                      units.add(d.meta());
                    }

                    if (!d.field(0).status().success()) {
                        return Optional.empty();
                    } else {
                        try {
                            JsonNode atr = MAPPER.readValue(d.field(0).value(), JsonNode.class);
                            JsonNode hlc = MAPPER.readValue(d.field(1).value(), JsonNode.class);
                            ParsedHLC parsedHLC = new ParsedHLC(hlc);

                            ActiveTransactionRecordEntry entry = createFrom(atrCollection.bucket(),
                                    atrId,
                                    atr,
                                    attemptId,
                                    parsedHLC.nowInNanos());
                            return Optional.of(entry);
                        }
                        catch (Throwable err) {
                            // CBSE-10352
                            if (logger != null) {
                                logger.info("", "Hit error while decoding ATR {}.{}.{}.{} {} {}",
                                        atrCollection.bucket(), atrCollection.scope(), atrCollection.collection(), atrId, attemptId, DebugUtil.dbg(err));
                                logger.warn("Attempt to dump raw JSON of ATR entry:");
                                try {
                                    byte[] raw = d.field(0).value();
                                    String asStr = new String(raw, StandardCharsets.UTF_8);
                                    logger.info("", "Raw JSON: {}", asStr);
                                    byte[] rawHLC = d.field(1).value();
                                    String asStrHLC = new String(rawHLC, StandardCharsets.UTF_8);
                                    logger.info("", "Raw JSON HLC: {}", asStrHLC);
                                }
                                catch (Throwable e) {
                                    logger.info("", "Error while trying to read raw JSON: {}", DebugUtil.dbg(e));
                                }
                            }
                            // This implementation cannot proceed in the face of a corrupted ATR doc, so fast-fail the
                            // transaction
                            throw new RuntimeException(err);
                        }
                    }
                });
    }

    public static ActiveTransactionRecordEntry createFrom(String atrBucket,
                                                          String atrId,
                                                          JsonNode entry,
                                                          String attemptId,
                                                          long cas) {
        Objects.requireNonNull(entry);
        Objects.requireNonNull(attemptId);

        AttemptState state = AttemptState.convert(entry.path(ATR_FIELD_STATUS).textValue());

        Optional<ForwardCompatibility> fc = Optional.ofNullable(entry.path(ATR_FIELD_FORWARD_COMPATIBILITY))
                .map(ForwardCompatibility::new);

        Optional<DurabilityLevel> dl = Optional.ofNullable(entry.path(ATR_FIELD_DURABILITY_LEVEL).textValue())
                .map(DurabilityLevelUtil::convertDurabilityLevel);

        return new ActiveTransactionRecordEntry(
                atrBucket,
                atrId,
                attemptId,
                Optional.ofNullable(entry.path(ATR_FIELD_TRANSACTION_ID).textValue()),
                state,
                parseMutationCASField(entry.path(ATR_FIELD_START_TIMESTAMP).textValue()),
                parseMutationCASField(entry.path(ATR_FIELD_START_COMMIT).textValue()),
                parseMutationCASField(entry.path(ATR_FIELD_TIMESTAMP_COMPLETE).textValue()),
                parseMutationCASField(entry.path(ATR_FIELD_TIMESTAMP_ROLLBACK_START).textValue()),
                parseMutationCASField(entry.path(ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE).textValue()),
                Optional.ofNullable(entry.get(ATR_FIELD_EXPIRES_AFTER_MILLIS) == null ? null : entry.get(ATR_FIELD_EXPIRES_AFTER_MILLIS).intValue()),
                processDocumentIdArray(entry.get(ATR_FIELD_DOCS_INSERTED)),
                processDocumentIdArray(entry.get(ATR_FIELD_DOCS_REPLACED)),
                processDocumentIdArray(entry.get(ATR_FIELD_DOCS_REMOVED)),
                cas,
                fc,
                dl
        );
    }

    public static Optional<List<DocRecord>> processDocumentIdArray(@Nullable JsonNode array) {
        if (array == null) {
            return Optional.empty();
        }

        return Optional.of(StreamSupport.stream(array.spliterator(), false)
                .map(DocRecord::createFrom)
                .collect(Collectors.toList()));
    }


    // ${Mutation.CAS} is written by kvengine with 'macroToString(htonll(info.cas))'.  Discussed this with KV team and,
    // though there is consensus that this is off (htonll is definitely wrong, and a string is an odd choice), there are
    // clients (SyncGateway) that consume the current string, so it can't be changed.  Note that only little-endian
    // servers are supported for Couchbase, so the 8 byte long inside the string will always be little-endian ordered.
    //
    // Looks like: "0x000058a71dd25c15"
    // Want:        0x155CD21DA7580000   (1539336197457313792 in base10, an epoch time in millionths of a second)
    public static long parseMutationCAS(String in) {
        return NANOSECONDS.toMillis(
                Long.reverseBytes(
                        Long.parseUnsignedLong(CbStrings.removeStart(in, "0x"), 16)));
    }

    public static Optional<Long> parseMutationCASField(String str) {
        if (str == null) {
            return Optional.empty();
        }
        return Optional.of(parseMutationCAS(str));
    }


    /**
     * TXNJ-13: Get the ATR.  The ATR's CAS will be as though a mutation had just been performed on that document.
     *
     * Note that MB-35388 only provides one-second granularity.
     */
    public static Mono<Optional<ActiveTransactionRecords>> getAtr(Core core,
                                                                  CollectionIdentifier atrCollection,
                                                                  String atrId,
                                                                  Duration timeout,
                                                                  @Nullable SpanWrapper pspan) {
        return TransactionKVHandler.lookupIn(core, atrCollection, atrId, timeout, false,  createClientContext("ATR::getAtr"), pspan,
        false,
        Arrays.asList(
                new SubdocGetRequest.Command(SubdocCommandType.GET, ATR_FIELD_ATTEMPTS, true, 0),
                new SubdocGetRequest.Command(SubdocCommandType.GET, "$vbucket.HLC", true, 1)
        ))


            // Possible results here:
            // If the server supports "$vbucket" / MB-35388: all fine, it is returned
            // Else if java-client >= 3.0.3: raises XattrUnknownVirtualAttributeException
            // Else it is passed down directly, and memcached disconnects
            // Hence there is effectively now a hard dependency on java-client 3.0.3+
            // Note that this code is only performed in protocol 2 which has a hard dependency on 6.6, so MB-35388
            // is certainly available.  And there is now a hard dependency on higher versions of java-client than 3.0.3.
            // So this code should always be safe.
            .map(d -> {
                try {
                    JsonNode attempts = MAPPER.readValue(d.field(0).value(), JsonNode.class);
                    JsonNode hlc = MAPPER.readValue(d.field(1).value(), JsonNode.class);
                    ParsedHLC parsedHLC = new ParsedHLC(hlc);

                    return Optional.of(mapToAtr(atrCollection, atrId, attempts, parsedHLC.nowInNanos(), parsedHLC.mode()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })

            .onErrorResume(err -> {
                // Don't capture RequestCancelledException here: it indicates using java-client < 3.0.3 on a pre-6.6 server, leading to
                // memcached continuously disconnecting.
                // Don't capture XattrUnknownVirtualAttributeException either, indicating we're on a pre-6.6 server..
                // Both the server and java-client versions are better tested at an earlier point.
                if (err instanceof DocumentNotFoundException) {
                    return Mono.just(Optional.empty());
                } else {
                    return Mono.error(err);
                }
            });
    }

    private static ActiveTransactionRecords mapToAtr(CollectionIdentifier atrCollection, String atrId, JsonNode attempts, long cas, CasMode casMode) {
        List<ActiveTransactionRecordEntry> entries = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        attempts.fields(),
                        Spliterator.ORDERED), false)
                .map(jo -> {
                    return createFrom(atrCollection.bucket(), atrId, jo.getValue(), jo.getKey(), cas);
                })
                .collect(Collectors.toList());

        return new ActiveTransactionRecords(atrId, atrCollection, cas, entries, casMode);
    }

    @Stability.Internal
    public static class ParsedHLC {
        private final CasMode mode;
        private final long nowInNanos;

        public ParsedHLC(JsonNode hlc) {
            String nowStr = hlc.path("now").textValue();
            String modeStr = hlc.path("mode").textValue();
            if (modeStr != null && modeStr.length() > 0) {
                switch (modeStr.charAt(0)) {
                    case 'l':
                        mode = CasMode.LOGICAL;
                        break;
                    case 'r':
                        mode = CasMode.REAL;
                        break;
                    default:
                        mode = CasMode.UNKNOWN;
                        break;
                }
            }
            else {
                mode = CasMode.UNKNOWN;
            }

            // Time is in seconds since unix epoch.  Convert into nanoseconds (same as CAS)
            // cas:        1582117988980686848
            // nowSeconds  1582117992
            // nowInNanos: 1582117992000000000
            long nowSeconds = Long.parseLong(nowStr);
            nowInNanos = TimeUnit.SECONDS.toNanos(nowSeconds);
        }

        public CasMode mode() {
            return mode;
        }

        public long nowInNanos() {
            return nowInNanos;
        }
    }
}
