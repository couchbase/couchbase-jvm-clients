package com.couchbase.client.java.kv;

import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.util.Bytes;

/**
 * An intention to perform a SubDocument remove operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class Remove extends MutateInSpec {
    private final String path;
    private boolean xattr = false;

    Remove(String path) {
        this.path = path;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public Remove xattr() {
        xattr = true;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        return new SubdocMutateRequest.Command(
                SubdocCommandType.DELETE,
                path,
                Bytes.EMPTY_BYTE_ARRAY,
                false,
                xattr,
                false
        );
    }
}
