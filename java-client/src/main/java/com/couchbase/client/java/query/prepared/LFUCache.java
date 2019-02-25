package com.couchbase.client.java.query.prepared;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Least frequently used cache
 *
 * @since 3.0.0
 */
public class LFUCache<K, V> {

	private final Map<K, CacheNode<K, V>> cache;
	private final LinkedHashSet<CacheNode<K, V>>[] frequencyList;
	private int lowestFrequency;
	private int maxFrequency;

	private final int maxCacheSize;

	public LFUCache(int maxCacheSize) {
		this.cache = new HashMap<>(maxCacheSize);
		this.frequencyList = new LinkedHashSet[maxCacheSize];
		this.lowestFrequency = 0;
		this.maxFrequency = maxCacheSize - 1;
		this.maxCacheSize = maxCacheSize;
		initFrequencyList();
	}

	public V put(K key, V value) {
		V oldV = null;
		CacheNode<K, V> currentNode = cache.get(key);
		if (currentNode == null) {
			if (cache.size() == maxCacheSize) {
				doEviction();
			}
			LinkedHashSet<CacheNode<K, V>> nodes = frequencyList[0];
			currentNode = new CacheNode<>(key, value, 0);
			nodes.add(currentNode);
			cache.put(key, currentNode);
			lowestFrequency = 0;
		} else {
			oldV = currentNode.v;
			currentNode.v = value;
		}
		return oldV;
	}

	public V get(Object k) {
		CacheNode<K, V> currentNode = cache.get(k);
		if (currentNode != null) {
			int currentFrequency = currentNode.frequency;
			if (currentFrequency < maxFrequency) {
				int nextFrequency = currentFrequency + 1;
				LinkedHashSet<CacheNode<K, V>> currentNodes = frequencyList[currentFrequency];
				LinkedHashSet<CacheNode<K, V>> newNodes = frequencyList[nextFrequency];
				moveToNextFrequency(currentNode, nextFrequency, currentNodes, newNodes);
				cache.put((K) k, currentNode);
				if (lowestFrequency == currentFrequency && currentNodes.isEmpty()) {
					lowestFrequency = nextFrequency;
				}
			} else {
				// Hybrid with LRU: put most recently accessed ahead of others:
				LinkedHashSet<CacheNode<K, V>> nodes = frequencyList[currentFrequency];
				nodes.remove(currentNode);
				nodes.add(currentNode);
			}
			return currentNode.v;
		} else {
			return null;
		}
	}

	public boolean contains(K key) {
		return cache.containsKey(key);
	}

	public V remove(Object k) {
		CacheNode<K, V> currentNode = cache.remove(k);
		if (currentNode != null) {
			LinkedHashSet<CacheNode<K, V>> nodes = frequencyList[currentNode.frequency];
			nodes.remove(currentNode);
			if (lowestFrequency == currentNode.frequency) {
				findNextLowestFrequency();
			}
			return currentNode.v;
		} else {
			return null;
		}
	}

	public void clear() {
		for (int i = 0; i <= maxFrequency; i++) {
			frequencyList[i].clear();
		}
		cache.clear();
		lowestFrequency = 0;
	}

	private void initFrequencyList() {
		for (int i = 0; i <= maxFrequency; i++) {
			frequencyList[i] = new LinkedHashSet<>();
		}
	}

	private void doEviction() {
		LinkedHashSet<CacheNode<K, V>> nodes = frequencyList[lowestFrequency];
		if (nodes.isEmpty()) {
			throw new IllegalStateException("Lowest frequency constraint");
		} else {
			Iterator<CacheNode<K, V>> it = nodes.iterator();
			if (it.hasNext()) {
				CacheNode<K, V> node = it.next();
				it.remove();
				cache.remove(node.k);
			} else {
				findNextLowestFrequency();
			}
		}
	}

	private void moveToNextFrequency(CacheNode<K, V> currentNode, int nextFrequency, LinkedHashSet<CacheNode<K, V>> currentNodes, LinkedHashSet<CacheNode<K, V>> newNodes) {
		currentNodes.remove(currentNode);
		newNodes.add(currentNode);
		currentNode.frequency = nextFrequency;
	}

	private void findNextLowestFrequency() {
		while (lowestFrequency <= maxFrequency && frequencyList[lowestFrequency].isEmpty()) {
			lowestFrequency++;
		}
		if (lowestFrequency > maxFrequency) {
			lowestFrequency = 0;
		}
	}

	private static class CacheNode<K, V> {

		public final K k;
		public V v;
		public int frequency;

		public CacheNode(K k, V v, int frequency) {
			this.k = k;
			this.v = v;
			this.frequency = frequency;
		}
	}
}