package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;

/**
 * An intention to perform a SubDocument increment operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class Increment extends MutateInSpec {
    private final String path;
    private final byte[] delta;
    private boolean xattr = false;
    private boolean createPath = false;

    Increment(String path, final byte[] delta) {
        this.path = path;
        this.delta = delta;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public Increment xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public Increment createPath() {
        createPath = true;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        return new SubdocMutateRequest.Command(
                SubdocCommandType.COUNTER,
                path,
                delta,
                createPath,
                xattr,
                false
        );
    }
}
