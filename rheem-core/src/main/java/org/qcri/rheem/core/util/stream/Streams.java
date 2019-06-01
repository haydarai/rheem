package org.qcri.rheem.core.util.stream;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class Streams {
    /**
     * Returns a sequential {@link Stream} of the contents of {@code iterable}, delegating to {@link
     * Collection#stream} if possible.
     */
    public static <T> Stream<T> stream(Iterable<T> iterable) {
        return (iterable instanceof Collection)
                ? ((Collection<T>) iterable).stream()
                : StreamSupport.stream(iterable.spliterator(), false);
    }

    public static <T> Stream<T> stream(Collection<T> collection) {
        return collection.stream();
    }

    /**
     * Returns a sequential {@link Stream} of the remaining contents of {@code iterator}. Do not use
     * {@code iterator} directly after passing it to this method.
     */
    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

}