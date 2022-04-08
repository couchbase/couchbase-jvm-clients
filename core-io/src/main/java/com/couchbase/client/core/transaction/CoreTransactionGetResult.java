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

package com.couchbase.client.core.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.transaction.components.DocumentMetadata;
import com.couchbase.client.core.transaction.components.OperationTypes;
import com.couchbase.client.core.transaction.components.TransactionLinks;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.util.DebugUtil;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;


/**
 * Represents a value fetched from Couchbase, along with additional transactional metadata.
 */
@Stability.Internal
public class CoreTransactionGetResult {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final @Nullable
    TransactionLinks links;

    // This is needed for provide {BACKUP-FIELDS}.
    // It is not available if fetch (or any operation) from query. Or on createFromInsert.  Or dehydrating serialized transaction.
    private final Optional<DocumentMetadata> documentMetadata;

    // CRC32 of body of document, at time of the most recent fetch.  (Could have get -> replace -> replace, and it would
    // be the CRC32 from the get).
    // It is stored separately from DocumentMetadata as it may come either from $document.value_crc32c or from query.
    // There are many times this will not be available - see getDocChanged for a list.
    private final Optional<String> crc32OfGet;

    // This is always JSON
    private byte[] content;

    private long cas;
    private final String id;
    private final CollectionIdentifier collection;

    private final Optional<JsonNode> txnMeta;

    /**
     * @param content will be nullable for tombstones, and some niche cases like REMOVE to REPLACE on same doc
     */
    @Stability.Internal
    public CoreTransactionGetResult(String id,
                                    @Nullable byte[] content,
                                    long cas,
                                    CollectionIdentifier collection,
                                    @Nullable TransactionLinks links,
                                    Optional<DocumentMetadata> documentMetadata,
                                    Optional<JsonNode> txnMeta,
                                    Optional<String> crc32OfGet) {
        this.id = Objects.requireNonNull(id);
        this.content = content;
        this.cas = cas;
        this.collection = Objects.requireNonNull(collection);
        this.links = links;
        this.documentMetadata = Objects.requireNonNull(documentMetadata);
        this.txnMeta = Objects.requireNonNull(txnMeta);
        this.crc32OfGet = Objects.requireNonNull(crc32OfGet);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TransactionGetResultInternal{");
        sb.append("id=").append(DebugUtil.docId(collection, id));
        sb.append(",cas=").append(cas);
        sb.append(",bucket=").append(collection.bucket());
        sb.append(",scope=").append(collection.scope());
        sb.append(",coll=").append(collection.collection());
        sb.append(",links=").append(links);
        sb.append(",txnMeta=").append(txnMeta);
        sb.append('}');
        return sb.toString();
    }

    public Optional<DocumentMetadata> documentMetadata() {
        return documentMetadata;
    }

    @Nullable public TransactionLinks links() {
        return links;
    }

    Optional<JsonNode> txnMeta() {
        return txnMeta;
    }

    /**
     * Returns the document's ID, which must be unique across the bucket.
     */
    public String id() {
        return id;
    }

    /**
     * Returns the document's CAS (compare-and-swap) value, used for optimistic concurrency.
     */
    public long cas() {
        return cas;
    }
    
    /**
     * Returns the raw unconverted contents as a byte[].
     */
    @Stability.Uncommitted
    public byte[] contentAsBytes() {
        return content;
    }

    CoreTransactionGetResult cas(long cas) {
        this.cas = cas;
        return this;
    }

    public Optional<String> crc32OfGet() {
        return crc32OfGet;
    }
    
    @Stability.Internal
    static CoreTransactionGetResult createFromInsert(CollectionIdentifier collection,
                                                     String id,
                                                     byte[] content,
                                                     String transactionId,
                                                     String attemptId,
                                                     String atrId,
                                                     String atrBucketName,
                                                     String atrScopeName,
                                                     String atrCollectionName,
                                                     long cas) {
        TransactionLinks links = new TransactionLinks(
                Optional.of(new String(content, CharsetUtil.UTF_8)),
                Optional.ofNullable(atrId),
                Optional.ofNullable(atrBucketName),
                Optional.ofNullable(atrScopeName),
                Optional.ofNullable(atrCollectionName),
                Optional.of(transactionId),
                Optional.of(attemptId),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(OperationTypes.INSERT),
                true,
                Optional.empty(),
                Optional.empty(),
                // The staged operationId is only used after reading the document so is unimportant here
                Optional.empty()
        );

        CoreTransactionGetResult out = new CoreTransactionGetResult(id,
                content,
                cas,
                collection,
                links,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        );

        return out;
    }

    @Stability.Internal
    public static CoreTransactionGetResult createFrom(CoreTransactionGetResult doc,
                                                      byte[] content) {
        TransactionLinks links = new TransactionLinks(
                doc.links.stagedContent(),
                doc.links.atrId(),
                doc.links.atrBucketName(),
                doc.links.atrScopeName(),
                doc.links.atrCollectionName(),
                doc.links.stagedTransactionId(),
                doc.links.stagedAttemptId(),
                doc.links.casPreTxn(),
                doc.links.revidPreTxn(),
                doc.links.exptimePreTxn(),
                doc.links.op(),
                doc.links.isDeleted(),
                doc.links.crc32OfStaging(),
                doc.links.forwardCompatibility(),
                doc.links.stagedOperationId()
        );

        CoreTransactionGetResult out = new CoreTransactionGetResult(doc.id,
                content,
                doc.cas,
                doc.collection,
                links,
                doc.documentMetadata,
                Optional.empty(),
                doc.crc32OfGet
        );

        return out;

    }

    @Stability.Internal
    public static CoreTransactionGetResult createFrom(CollectionIdentifier collection,
                                                      String documentId,
                                                      SubdocGetResponse doc) throws IOException {
        Optional<String> atrId = Optional.empty();
        Optional<String> transactionId = Optional.empty();
        Optional<String> attemptId = Optional.empty();
        Optional<String> operationId = Optional.empty();
        Optional<String> stagedContent = Optional.empty();
        Optional<String> atrBucketName = Optional.empty();
        Optional<String> atrScopeName = Optional.empty();
        Optional<String> atrCollectionName = Optional.empty();
        Optional<String> crc32OfStaging = Optional.empty();
        Optional<ForwardCompatibility> forwardCompatibility = Optional.empty();

        // Read from xattrs.txn.restore
        Optional<String> casPreTxn = Optional.empty();
        Optional<String> revidPreTxn = Optional.empty();
        Optional<Long> exptimePreTxn = Optional.empty();

        Optional<String> op = Optional.empty();

        // "txn.id"
        if (doc.values()[0].status().success()) {
            JsonNode id = MAPPER.readValue(doc.values()[0].value(), JsonNode.class);
            transactionId = Optional.ofNullable(id.path("txn").textValue());
            attemptId = Optional.ofNullable(id.path("atmpt").textValue());
            operationId = Optional.ofNullable(id.path("op").textValue());
        }

        // "txn.atr"
        if (doc.values()[1].status().success()) {
            JsonNode atr  = MAPPER.readValue(doc.values()[1].value(), JsonNode.class);
            atrId = Optional.ofNullable(atr.path("id").textValue());
            atrBucketName = Optional.ofNullable(atr.path("bkt").textValue());
            String scope = atr.path("scp").textValue();
            String coll = atr.path("coll").textValue();

            if (scope != null) {
                atrScopeName = Optional.ofNullable(scope);
                atrCollectionName = Optional.ofNullable(coll);
            }
            else {
                // Legacy protocol 1 support
                String[] splits = coll.split("\\.");
                atrScopeName = Optional.of(splits[0]);
                atrCollectionName = Optional.of(splits[1]);
            }
        }

        // "txn.op.type"
        if (doc.values()[2].status().success()) {
            op = Optional.of(Mapper.reader().readValue(doc.values()[2].value(), String.class));
        }

        // "txn.op.stgd"
        if (doc.values()[3].status().success()) {
            byte[] raw = doc.values()[3].value();
            String str = new String(raw, CharsetUtil.UTF_8);
            stagedContent = Optional.of(str);
        }

        // "txn.op.crc32"
        if (doc.values()[4].status().success()) {
            crc32OfStaging = Optional.of(Mapper.reader().readValue(doc.values()[4].value(), String.class));
        }

        // "txn.restore"
        if (doc.values()[5].status().success()) {
            JsonNode restore  = MAPPER.readValue(doc.values()[5].value(), JsonNode.class);
            casPreTxn = Optional.of(restore.path("CAS").textValue());
            // Only present in 6.5+
            revidPreTxn = Optional.of(restore.path("revid").textValue());
            exptimePreTxn = Optional.of(restore.path("exptime").asLong());
        }

        // "txn.fc"
        if (doc.values()[6].status().success()) {
            JsonNode json = MAPPER.readValue(doc.values()[6].value(), JsonNode.class);
            ForwardCompatibility fc = new ForwardCompatibility(json);
            forwardCompatibility = Optional.of(fc);
        }

        if (!doc.values()[7].status().success()) {
            throw new IllegalStateException("$document requested but not received");
        }
        // Read from $document
        JsonNode restore = MAPPER.readValue(doc.values()[7].value(), JsonNode.class);
        String casFromDocument = restore.path("CAS").textValue();
        // Only present in 6.5+
        String revidFromDocument = restore.path("revid").textValue();
        Long exptimeFromDocument = restore.path("exptime").longValue();
        String crc32FromDocument = restore.path("value_crc32c").textValue();

        byte[] content;

        // body
        if (doc.values()[8].status().success()) {
            content = doc.values()[8].value();
        }
        else {
            content = new byte[] {};
        }

        TransactionLinks links = new TransactionLinks(
                stagedContent,
                atrId,
                atrBucketName,
                atrScopeName,
                atrCollectionName,
                transactionId,
                attemptId,
                casPreTxn,
                revidPreTxn,
                exptimePreTxn,
                op,
                doc.isDeleted(),
                crc32OfStaging,
                forwardCompatibility,
                operationId);

        DocumentMetadata md = new DocumentMetadata(
                casFromDocument,
                revidFromDocument,
                exptimeFromDocument);

        CoreTransactionGetResult out = new CoreTransactionGetResult(documentId,
                content,
                doc.cas(),
                collection,
                links,
                Optional.of(md),
                Optional.empty(),
                Optional.of(crc32FromDocument));

        return out;
    }

    public CollectionIdentifier collection() {
        return collection;
    }
}
