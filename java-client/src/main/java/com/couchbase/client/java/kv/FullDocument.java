package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;

/**
 * An intention to perform a SubDocument operation to upload or insert a full document.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class FullDocument extends MutateInSpec {
    private final EncodedDocument doc;

    FullDocument(EncodedDocument doc) {
        this.doc = doc;
    }

    public SubdocMutateRequest.Command encode() {
        return new SubdocMutateRequest.Command(
                SubdocCommandType.SET_DOC,
                "",
                doc.content(),
                false,
                false,
                false
        );
    }
}
