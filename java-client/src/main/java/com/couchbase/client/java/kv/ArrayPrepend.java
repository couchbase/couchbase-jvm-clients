package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;

/**
 * An intention to perform a SubDocument array prepend operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class ArrayPrepend extends MutateInSpec {
    private final String path;
    private final EncodedDocument doc;
    private boolean xattr = false;
    private boolean expandMacro = false;
    private boolean createPath = false;

    ArrayPrepend(String path, EncodedDocument doc) {
        this.path = path;
        this.doc = doc;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public ArrayPrepend xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public ArrayPrepend createPath() {
        createPath = true;
        return this;
    }

    /**
     * Sets that this contains a macro that should be expanded on the server.  For internal use.
     * @return this, for chaining
     */
    @Stability.Internal
    public ArrayPrepend expandMacro() {
        expandMacro = true;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        return new SubdocMutateRequest.Command(
                SubdocCommandType.ARRAY_PUSH_FIRST,
                path,
                doc.content(),
                createPath,
                xattr,
                expandMacro
        );
    }
}
