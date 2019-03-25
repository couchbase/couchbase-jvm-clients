package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.Encoder;

/**
 * An intention to perform a SubDocument array append operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class ArrayAppend extends MutateInSpec {
    private final String path;
    private final Object doc;
    private boolean xattr = false;
    private boolean expandMacro = false;
    private boolean createPath = false;
    private Encoder encoder = EncoderUtil.ENCODER;

    ArrayAppend(String path, Object doc) {
        this.path = path;
        this.doc = doc;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public ArrayAppend xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public ArrayAppend createPath() {
        createPath = true;
        return this;
    }

    /**
     * Sets a custom encoder, allowing any data type to be encoded.
     * @param encoder the custom Encoder
     * @return this, for chaining
     */
    public ArrayAppend encoder(Encoder encoder) {
        this.encoder = encoder;
        return this;
    }

    /**
     * Sets that this contains a macro that should be expanded on the server.  For internal use.
     * @return this, for chaining
     */
    @Stability.Internal
    public ArrayAppend expandMacro() {
        expandMacro = true;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        EncodedDocument content = encoder.encode(doc);

        return new SubdocMutateRequest.Command(
                SubdocCommandType.ARRAY_PUSH_LAST,
                path,
                content.content(),
                createPath,
                xattr,
                expandMacro
        );
    }
}
