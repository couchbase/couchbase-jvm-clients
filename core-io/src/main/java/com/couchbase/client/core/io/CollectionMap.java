package com.couchbase.client.core.io;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link CollectionMap} maps a locator to the encoded collection ID representation.
 */
public class CollectionMap extends ConcurrentHashMap<CollectionIdentifier, byte[]> {
}
