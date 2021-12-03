import partition.Initializer;
import partition.TxnProcessor;
import storage.LockManager;
import storage.Store;
import zookeeper.Executor;//package Zookeeper.executor

public class Application {
    public static void main(String[] args){
        Executor connection = new Executor("localhost");
        int noOfPartitions = 10;
        Initializer initializer = new Initializer(connection);
        Store store = new Store();
        initializer.initializePartitions(noOfPartitions,store);
        LockManager lockManager = new LockManager();
        initializer.getShardMap().forEach((key, value) -> {
            TxnProcessor processor = new TxnProcessor(key,store,lockManager,value.getRange());
            value.setTxnProcessor(processor);
        });
        initializer.getShardMap().forEach((key, value) -> value.pushToProduceQueue());
    }
}
