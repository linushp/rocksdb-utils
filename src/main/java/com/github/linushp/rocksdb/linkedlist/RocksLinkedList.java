package com.github.linushp.rocksdb.linkedlist;

import com.github.linushp.rocksdb.utils.NodeKeyManager;
import com.github.linushp.rocksdb.utils.RocksFsBase;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RocksLinkedList extends RocksFsBase {

    private final RocksLinkedListMetaData metaData;
    private final NodeKeyManager nodeKeyManager;
    private static final Map<byte[], RocksLinkedList> instanceMap = new HashMap<>();

    public static synchronized RocksLinkedList getInstance(RocksDB rocksDB, byte[] keyBytes) throws Exception {
        RocksLinkedList instance = instanceMap.get(keyBytes);
        if (instance == null) {
            instance = new RocksLinkedList(rocksDB, keyBytes);
            instanceMap.put(keyBytes, instance);
        }
        return instance;
    }

    public static synchronized RocksLinkedList getInstance(RocksDB rocksDB, String key) throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return getInstance(rocksDB, keyBytes);
    }

    private RocksLinkedList(RocksDB rocksDB, byte[] keyBytes) throws Exception {
        super(rocksDB);
        this.nodeKeyManager = NodeKeyManager.getInstance(rocksDB, keyBytes);
        byte[] metaDataNodeKey = this.nodeKeyManager.getMetaDataNodeKey();
        this.metaData = RocksLinkedListMetaData.readOrCreateMetaData(rocksDB, metaDataNodeKey);
    }


    /**
     * Links e as first element.
     */
    private synchronized void linkFirst(byte[] e) throws Exception {

        final RocksLinkedListNode f = metaData.getFirstNode();
        final RocksLinkedListNode newNode = newRocksLinkedListNode(null, e, f);
        metaData.setFirstNodeKey(newNode.getNodeKey());
        if (f == null) {
            metaData.setLastNodeKey(newNode.getNodeKey());
        } else {
            f.setPrevNodeKey(newNode.getNodeKey());
        }
        metaData.setSize(metaData.getSize() + 1);
        metaData.setModCount(metaData.getModCount() + 1);

        saveRocksNode(metaData);
        saveRocksNode(newNode);
        saveRocksNode(f);
    }


    private RocksLinkedListNode newRocksLinkedListNode(RocksLinkedListNode prevNode, byte[] data, RocksLinkedListNode nextNode) throws Exception {
        byte[] nodeKey = this.nodeKeyManager.getNextNodeKey();
        return new RocksLinkedListNode(this.rocksDB, nodeKey, prevNode, data, nextNode);
    }


    public synchronized void addFirst(byte[] byteArray) {
        try {
            linkFirst(byteArray);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public synchronized void addLast(byte[] byteArray) {
        try {
            linkLast(byteArray);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized void linkBefore(byte[] e, RocksLinkedListNode succ) throws Exception {

        final RocksLinkedListNode pred = succ.getPrevNode();
        final RocksLinkedListNode newNode = newRocksLinkedListNode(pred, e, succ);

        succ.setPrevNode(newNode);

        if (pred == null) {
            metaData.setFirstNode(newNode);
        } else {
            pred.setNextNode(newNode);
        }

        metaData.setSize(metaData.getSize() + 1);
        metaData.setModCount(metaData.getModCount() + 1);

        saveRocksNode(metaData);
        saveRocksNode(succ);
        saveRocksNode(pred);

    }

    private synchronized void linkLast(byte[] e) throws Exception {

        final RocksLinkedListNode l = getRocksLinkedListNode(metaData.getLastNodeKey());

        final RocksLinkedListNode newNode = newRocksLinkedListNode(l, e, null);
        metaData.setLastNodeKey(newNode.getNodeKey());
        if (l == null) {
            metaData.setFirstNodeKey(newNode.getNodeKey());
        } else {
            l.setNextNodeKey(newNode.getNodeKey());
        }

        metaData.setSize(metaData.getSize() + 1);
        metaData.setModCount(metaData.getModCount() + 1);

        saveRocksNode(metaData);
        saveRocksNode(newNode);
        saveRocksNode(l);

    }


    public synchronized boolean offerFirst(byte[] e) {
        addFirst(e);
        return true;
    }


    public synchronized boolean offerLast(byte[] e) {
        addLast(e);
        return true;
    }


    public synchronized byte[] removeFirst() {
        final RocksLinkedListNode f = getRocksLinkedListNode(metaData.getFirstNodeKey());
        if (f == null)
            throw new NoSuchElementException();
        return unlinkFirst(f);
    }

    private synchronized byte[] unlinkFirst(RocksLinkedListNode f) {
        // assert f == first && f != null;
        final byte[] element = f.getData();
        final RocksLinkedListNode next = f.getNextNode();
        f.setData(null);
        f.setNextNodeKey(null); // help GC

        metaData.setFirstNode(next);
        if (next == null) {
            metaData.setLastNode(null);
        } else {
            next.setPrevNode(null);
        }
        metaData.setSize(metaData.getSize() - 1);
        metaData.setModCount(metaData.getModCount() + 1);

        saveRocksNode(metaData);
        saveRocksNode(next);
        deleteRocksNode(f);

        return element;

    }


    public synchronized byte[] removeLast() {
        final RocksLinkedListNode l = metaData.getLastNode();
        if (l == null)
            throw new NoSuchElementException();
        return unlinkLast(l);
    }

    private synchronized byte[] unlinkLast(RocksLinkedListNode l) {

        final byte[] element = l.getData();
        final RocksLinkedListNode prev = l.getPrevNode();
        l.setData(null);
        l.setPrevNode(null);

        metaData.setLastNode(prev);
        if (prev == null) {
            metaData.setFirstNode(null);
        } else {
            prev.setNextNode(null);
        }

        metaData.setSize(metaData.getSize() - 1);
        metaData.setModCount(metaData.getModCount() + 1);

        return element;

    }


    public synchronized byte[] pollFirst() {
        final RocksLinkedListNode f = metaData.getFirstNode();
        return (f == null) ? null : unlinkFirst(f);
    }


    public synchronized byte[] pollLast() {
        final RocksLinkedListNode l = metaData.getLastNode();
        return (l == null) ? null : unlinkLast(l);
    }


    public synchronized byte[] getFirst() {
        final RocksLinkedListNode f = metaData.getFirstNode();
        if (f == null)
            throw new NoSuchElementException();
        return f.getData();
    }


    public synchronized byte[] getLast() {
        final RocksLinkedListNode l = metaData.getLastNode();
        if (l == null)
            throw new NoSuchElementException();
        return l.getData();
    }


    public synchronized byte[] peekFirst() {
        final RocksLinkedListNode f = metaData.getFirstNode();
        return (f == null) ? null : f.getData();
    }


    public synchronized byte[] peekLast() {
        final RocksLinkedListNode l = metaData.getLastNode();
        return (l == null) ? null : l.getData();
    }


    public synchronized boolean removeFirstOccurrence(Object o) {
        return remove(o);
    }


    public synchronized boolean removeLastOccurrence(Object o) {

        if (o == null) {
            for (RocksLinkedListNode x = metaData.getLastNode(); x != null; x = x.getPrevNode()) {
                if (x.getData() == null) {
                    unlink(x);
                    return true;
                }
            }
        } else {
            for (RocksLinkedListNode x = metaData.getLastNode(); x != null; x = x.getPrevNode()) {
                if (o.equals(x.getData())) {
                    unlink(x);
                    return true;
                }
            }
        }
        return false;

    }

    private synchronized byte[] unlink(RocksLinkedListNode x) {

        // assert x != null;
        final byte[] element = x.getData();
        final RocksLinkedListNode next = x.getNextNode();
        final RocksLinkedListNode prev = x.getPrevNode();

        if (prev == null) {
            metaData.setFirstNode(next);
        } else {
            prev.setNextNode(next);
            x.setPrevNode(null);
        }

        if (next == null) {
            metaData.setLastNode(prev);
        } else {
            next.setPrevNode(prev);
            x.setNextNode(null);
        }

        x.setData(null);

        metaData.setSize(metaData.getSize() - 1);
        metaData.setModCount(metaData.getModCount() + 1);


        saveRocksNode(metaData);
        saveRocksNode(next);
        saveRocksNode(prev);
        deleteRocksNode(x);

        return element;

    }


    public synchronized boolean offer(byte[] e) {
        return add(e);
    }


    public synchronized byte[] remove() {
        return removeFirst();
    }


    public synchronized byte[] poll() {
        final RocksLinkedListNode f = metaData.getFirstNode();
        return (f == null) ? null : unlinkFirst(f);

    }


    public synchronized byte[] element() {
        return getFirst();
    }


    public synchronized byte[] peek() {
        final RocksLinkedListNode f = metaData.getFirstNode();
        return (f == null) ? null : f.getData();
    }


    public synchronized void push(byte[] e) {
        addFirst(e);
    }


    public synchronized byte[] pop() {
        return removeFirst();
    }


    public synchronized Iterator<byte[]> descendingIterator() {
        return new DescendingIterator();
    }

    /**
     * Adapter to provide descending iterators via ListItr.previous
     */
    private class DescendingIterator implements Iterator<byte[]> {
        private final ListItr itr = new ListItr(size());

        public boolean hasNext() {
            return itr.hasPrevious();
        }

        public byte[] next() {
            return itr.previous();
        }

        public void remove() {
            itr.remove();
        }
    }


    public synchronized int size() {
        return metaData.getSize();
    }


    public synchronized boolean isEmpty() {
        return this.size() == 0;
    }


    public synchronized Iterator<byte[]> iterator() {
        return this.listIterator();
    }


    public synchronized Object[] toArray() {
        int size = metaData.getSize();
        Object[] result = new Object[size];
        int i = 0;
        for (RocksLinkedListNode x = metaData.getFirstNode(); x != null; x = x.getNextNode()) {
            result[i++] = x.getData();
        }
        return result;
    }


    public synchronized boolean add(byte[] e) {
        try {
            linkLast(e);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return false;
    }


    public synchronized boolean remove(Object o) {

        if (o == null) {
            for (RocksLinkedListNode x = metaData.getFirstNode(); x != null; x = x.getNextNode()) {
                if (x.getData() == null) {
                    unlink(x);
                    return true;
                }
            }
        } else {
            for (RocksLinkedListNode x = metaData.getFirstNode(); x != null; x = x.getNextNode()) {
                if (o.equals(x.getData())) {
                    unlink(x);
                    return true;
                }
            }
        }
        return false;
    }


    public synchronized boolean addAll(Collection<? extends byte[]> c) {
        return addAll(metaData.getSize(), c);
    }


    public synchronized boolean addAll(int index, Collection<? extends byte[]> c) {
        checkPositionIndex(index);

        Object[] a = c.toArray();
        int numNew = a.length;
        if (numNew == 0)
            return false;

        RocksLinkedListNode pred, succ;

        if (index == metaData.getSize()) {
            succ = null;
            pred = metaData.getLastNode();
        } else {
            succ = node(index);
            pred = succ.getPrevNode();
        }

        for (Object o : a) {
            @SuppressWarnings("unchecked") byte[] e = (byte[]) o;
            RocksLinkedListNode newNode = null;
            try {
                newNode = newRocksLinkedListNode(pred, e, null);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            if (pred == null) {
                metaData.setFirstNode(newNode);
            } else {
                pred.setNextNode(newNode);
            }
            pred = newNode;

            saveRocksNode(newNode);
        }

        if (succ == null) {
            metaData.setLastNode(pred);
        } else {
            pred.setNextNode(succ);
            succ.setPrevNode(pred);
        }

        metaData.setSize(metaData.getSize() + numNew);
        metaData.setModCount(metaData.getModCount() + 1);

        saveRocksNode(metaData);
        saveRocksNode(pred);
        saveRocksNode(succ);

        return true;
    }

    /**
     * Tells if the argument is the index of an existing element.
     */
    private boolean isElementIndex(int index) {
        return index >= 0 && index < metaData.getSize();
    }

    /**
     * Tells if the argument is the index of a valid position for an
     * iterator or an add operation.
     */
    private boolean isPositionIndex(int index) {
        return index >= 0 && index <= metaData.getSize();
    }

    /**
     * Constructs an IndexOutOfBoundsException detail message.
     * Of the many possible refactorings of the error handling code,
     * this "outlining" performs best with both server and client VMs.
     */
    private String outOfBoundsMsg(int index) {
        return "Index: " + index + ", Size: " + metaData.getSize();
    }

    private void checkElementIndex(int index) {
        if (!isElementIndex(index))
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
    }

    private void checkPositionIndex(int index) {
        if (!isPositionIndex(index))
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
    }


    /**
     * Returns the (non-null) Node at the specified element index.
     */
    private RocksLinkedListNode node(int index) {
        // assert isElementIndex(index);
        int size = metaData.getSize();

        if (index < (size >> 1)) {
            RocksLinkedListNode x = metaData.getFirstNode();
            for (int i = 0; i < index; i++) {
                x = x.getNextNode();
            }
            return x;
        } else {
            RocksLinkedListNode x = metaData.getLastNode();
            for (int i = size - 1; i > index; i--) {
                x = x.getPrevNode();
            }
            return x;
        }
    }


    public synchronized void quickClear() throws Exception {
        byte[] keyBegin = this.nodeKeyManager.getMinNodeKey();
        byte[] keyEnd = this.nodeKeyManager.getNextNodeKey();

        rocksDB.deleteRange(keyBegin, keyEnd);


        metaData.setLastNode(null);
        metaData.setFirstNode(null);
        metaData.setSize(0);
        metaData.setModCount(metaData.getModCount() + 1);
        saveRocksNode(metaData);
    }


    public synchronized void clear() {
        try {
            this.quickClear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public synchronized byte[] get(int index) {
        checkElementIndex(index);
        return node(index).getData();
    }


    public synchronized byte[] set(int index, byte[] element) {

        checkElementIndex(index);

        RocksLinkedListNode x = node(index);
        byte[] oldVal = x.getData();
        x.setData(element);

        saveRocksNode(x);

        return oldVal;
    }


    public synchronized void add(int index, byte[] element) {
        checkPositionIndex(index);

        if (index == metaData.getSize()) {
            try {
                linkLast(element);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                linkBefore(element, node(index));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public synchronized byte[] remove(int index) {
        checkElementIndex(index);
        return unlink(node(index));
    }


    public ListIterator<byte[]> listIterator() {
        return listIterator(0);
    }


    public ListIterator<byte[]> listIterator(int index) {
        checkPositionIndex(index);
        return new ListItr(index);
    }


    private class ListItr implements ListIterator<byte[]> {
        private RocksLinkedListNode lastReturned;
        private RocksLinkedListNode next;
        private int nextIndex;
        private int expectedModCount = metaData.getModCount();


        public ListItr(int index) {
            // assert isPositionIndex(index);
            next = (index == metaData.getSize()) ? null : node(index);
            nextIndex = index;
        }

        public boolean hasNext() {
            return nextIndex < metaData.getSize();
        }

        public synchronized byte[] next() {
            checkForComodification();
            if (!hasNext())
                throw new NoSuchElementException();

            lastReturned = next;
            next = next.getNextNode();
            nextIndex++;
            return lastReturned.getData();
        }

        public boolean hasPrevious() {
            return nextIndex > 0;
        }

        public byte[] previous() {
            checkForComodification();
            if (!hasPrevious())
                throw new NoSuchElementException();
            RocksLinkedListNode last = metaData.getLastNode();
            lastReturned = next = (next == null) ? last : next.getPrevNode();
            nextIndex--;
            return lastReturned.getData();
        }

        public int nextIndex() {
            return nextIndex;
        }

        public int previousIndex() {
            return nextIndex - 1;
        }

        public void remove() {
            checkForComodification();
            if (lastReturned == null)
                throw new IllegalStateException();

            RocksLinkedListNode lastNext = lastReturned.getNextNode();
            unlink(lastReturned);
            if (next == lastReturned)
                next = lastNext;
            else {
                nextIndex--;
            }
            lastReturned = null;
            expectedModCount++;
        }

        public void set(byte[] e) {
            if (lastReturned == null)
                throw new IllegalStateException();
            checkForComodification();
            lastReturned.setData(e);

            saveRocksNode(lastReturned);
        }

        public void add(byte[] e) {
            checkForComodification();
            lastReturned = null;
            if (next == null) {
                try {
                    linkLast(e);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            } else {
                try {
                    linkBefore(e, next);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
            nextIndex++;
            expectedModCount++;
        }

        final void checkForComodification() {
            if (metaData.getModCount() != expectedModCount)
                throw new ConcurrentModificationException();
        }
    }

}
