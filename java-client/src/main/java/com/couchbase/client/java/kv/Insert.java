package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;

/**
 * An intention to perform a SubDocument insert operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class Insert extends MutateInSpec {
    private final String path;
    private final EncodedDocument doc;
    private boolean xattr = false;
    private boolean expandMacro = false;
    private boolean createPath = false;

    Insert(String path, EncodedDocument doc) {
        this.path = path;
        this.doc = doc;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public Insert xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public Insert createPath() {
        createPath = true;
        return this;
    }

    /**
     * Sets that this contains a macro that should be expanded on the server.  For internal use.
     * @return this, for chaining
     */
    @Stability.Internal
    public Insert expandMacro() {
        expandMacro = true;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        return new SubdocMutateRequest.Command(
                SubdocCommandType.DICT_ADD,
                path,
                doc.content(),
                createPath,
                xattr,
                expandMacro
        );
    }
}
