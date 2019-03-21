package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;

/**
 * An intention to perform a SubDocument array insert operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class ArrayInsert extends MutateInSpec {
    private final String path;
    private final EncodedDocument doc;
    private boolean xattr = false;
    private boolean expandMacro = false;
    private boolean createPath = false;

    ArrayInsert(String path, EncodedDocument doc) {
        this.path = path;
        this.doc = doc;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public ArrayInsert xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public ArrayInsert createPath() {
        createPath = true;
        return this;
    }

    /**
     * Sets that this contains a macro that should be expanded on the server.  For internal use.
     * @return this, for chaining
     */
    @Stability.Internal
    public ArrayInsert expandMacro() {
        expandMacro = true;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        return new SubdocMutateRequest.Command(
                SubdocCommandType.ARRAY_INSERT,
                path,
                doc.content(),
                createPath,
                xattr,
                expandMacro
        );
    }
}
