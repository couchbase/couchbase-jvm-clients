package com.couchbase.client.performer.core.bounds;

import java.util.concurrent.atomic.AtomicInteger;

public class BoundsCounterEquals extends BoundsCounterBased {
    private int initialCounterValue;

    public BoundsCounterEquals(AtomicInteger counter) {
        super(counter);
        initialCounterValue = counter.get();
    }

    @Override
    public boolean canExecute() {
        return counter.get() == initialCounterValue;
    }
}
