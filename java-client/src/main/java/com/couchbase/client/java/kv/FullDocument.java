package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.Encoder;

/**
 * An intention to perform a SubDocument operation to upload or insert a full document.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class FullDocument extends MutateInSpec {
    private final Object doc;
    private Encoder encoder = EncoderUtil.ENCODER;

    FullDocument(Object doc) {
        this.doc = doc;
    }

    /**
     * Sets a custom encoder, allowing any data type to be encoded.
     * @param encoder the custom Encoder
     * @return this, for chaining
     */
    public FullDocument encoder(Encoder encoder) {
        this.encoder = encoder;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        EncodedDocument content = encoder.encode(doc);

        return new SubdocMutateRequest.Command(
                SubdocCommandType.SET_DOC,
                "",
                content.content(),
                false,
                false,
                false
        );
    }
}
