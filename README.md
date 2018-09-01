# rocksdb-utils


####  使用rocksDB实现LinkedList双向链表,可用于本地MQ队列，实现本地MQ等持久化存储。



## 使用DEMO

```
import com.github.linushp.rocksdb.linkedlist.RocksLinkedList;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

public class TestLinkedList {

    public static void main(String [] args) throws Exception {


        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true).setAllowMmapWrites(true);
        options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
        RocksDB rocksDB = RocksDB.open(options, "./rocksDbData");

        byte[] mask = new byte[]{0x10,0x20};
        RocksLinkedList linkedList = RocksLinkedList.getInstance(rocksDB,mask);

        linkedList.addLast("1".getBytes());
        linkedList.addLast("2".getBytes());
        linkedList.addLast("3".getBytes());
        linkedList.addLast("4".getBytes());

        RocksLinkedList linkedList2 = RocksLinkedList.getInstance(rocksDB,mask);
        byte[] x = linkedList2.pollFirst();
        System.out.println(new String(x));
    }
}

```