package storage;

import model.Transaction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.txn.Txn;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
enum LockMode {
    SHARED,
    EXCLUSIVE,
    UNLOCKED
}
class LockRequest{
    Transaction txn;
    LockMode mode;
    int partitionId;
    public LockRequest(Transaction txn,LockMode mode,int partitionId){
        this.txn = txn;
        this.mode =mode;
        this.partitionId = partitionId;
    }
}
class Tracker{
    int needed,acquired;
    Transaction txn;
    public Tracker(int needed,Transaction txn){
        this.needed = needed;
        this.acquired = 0;
        this.txn = txn;
    }
    public void incrementAcquired(){
        this.acquired++;
    }
    public boolean isReady(){
        return this.acquired >= this.needed;
    }
}
public class LockManager {
    private final ConcurrentHashMap<Integer, Deque<LockRequest>> lockTable ;
    private final Map<String, Tracker> locksAcquired;
    Map<Integer,ArrayBlockingQueue<Transaction>> ready_txns;

    public LockManager(Map<Integer,ArrayBlockingQueue<Transaction>> ready_txns){
        lockTable = new ConcurrentHashMap<>();
        this.ready_txns = ready_txns;
        locksAcquired = new ConcurrentHashMap<>();
        new Thread(this::checkForReadyTransaction).start();

    }

    private void checkForReadyTransaction() {
        ArrayList<String> doneTransactions = new ArrayList<>();
        while(true){
            if(locksAcquired.isEmpty()){
                continue;

            }
            locksAcquired.forEach((key,value) -> {
                if(value.isReady() ) {
                    addToReadyQueue(value.txn);
                    doneTransactions.add(key);
                }
            });
            doneTransactions.forEach(locksAcquired::remove);
            doneTransactions.clear();
        }
    }

    synchronized public boolean writeLock(Transaction txn,int key,int partitionId){
        boolean isLockGranted = false;
        initializeLockAcquired(txn);
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests == null || lockRequests.isEmpty() ){
            isLockGranted = true;
            addLockGranted(txn.getTransactionId());
        }

        LockRequest lockRequest = new LockRequest(txn,LockMode.EXCLUSIVE,partitionId);
        Deque<LockRequest> temp = new ConcurrentLinkedDeque<>();

        if(lockRequests == null){
            temp.addLast(lockRequest);
            lockTable.put(key,temp);
        }else{
            lockRequests.add(lockRequest);
        }
//        System.out.println("Write lock for transaction : " + txn.getTransactionId() + " Granted : " + isLockGranted + " key :" + key+ " Partition :" + partitionId);
        return isLockGranted;
    }
    public synchronized void addLockGranted(String id){
        if(locksAcquired.containsKey(id))
            locksAcquired.get(id).incrementAcquired();
//        System.out.println("Transaction :" + id + "...." + locksAcquired.get(id).acquired + "....." +locksAcquired.get(id).needed);
    }
    public synchronized void initializeLockAcquired(Transaction transaction){
        locksAcquired.putIfAbsent(transaction.getTransactionId(),new Tracker(transaction.getReadSet().size() + transaction.getWriteSet().size(),transaction));
    }

    synchronized public boolean readLock(Transaction txn,int key,int partitionId){
        boolean isLockGranted = false;
        initializeLockAcquired(txn);
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests == null || lockRequests.isEmpty()){
            isLockGranted = true;
        }
        LockRequest lockRequest = new LockRequest(txn, LockMode.SHARED,partitionId);
        Deque<LockRequest> temp = new ConcurrentLinkedDeque<>();
        if (lockRequests == null) {
            temp.addLast(lockRequest);
            lockTable.put(key, temp);
        } else {
            lockRequests.add(lockRequest);
        }
        lockRequests = lockTable.get(key);
        Iterator<LockRequest> it = lockRequests.iterator();
        boolean isExclusiveLock = LockMode.EXCLUSIVE.equals(it.next().mode);
        if(!isLockGranted && !isExclusiveLock){
            isLockGranted = true;
            while(it.hasNext()){
                if(!LockMode.SHARED.equals(it.next().mode)){
                    isLockGranted = false;
                    break;
                }
            }
        }
        if(isLockGranted){
            addLockGranted(txn.getTransactionId());
        }
//        System.out.println("Read lock for transaction : " + txn.getTransactionId() + " Granted : " + isLockGranted + " key :" + key + " Partition :" + partitionId);
        return isLockGranted;
    }

    public synchronized void removeLockAcquired(String id){
        locksAcquired.remove(id);
    }

    synchronized public void release(Transaction txn,int key,int partitionId){
//        System.out.println("Release Lock Request :" + txn.getTransactionId() + "Key :" + key);
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests == null || lockRequests.isEmpty())
            return;
        Iterator<LockRequest> it = lockRequests.iterator();
        while(it.hasNext()){
            LockRequest curr = it.next();
            if(curr.txn.getTransactionId().equals(txn.getTransactionId())){
                lockRequests.remove(curr);
                break;
            }
        }
        if(lockRequests.isEmpty()){
//            System.out.println("Empty : Released lock for transaction : " + txn.getTransactionId() + " Key :"  + key+ " Partition :" + partitionId);
            return;
        }

        it = lockRequests.iterator();
        boolean isShared = LockMode.SHARED.equals(lockRequests.getFirst().mode);

        if(!isShared && !lockRequests.getFirst().txn.getTransactionId().equals(txn.getTransactionId())){
            initializeLockAcquired(lockRequests.getFirst().txn);
            addLockGranted(lockRequests.getFirst().txn.getTransactionId());
//            System.out.println("Transaction : " + lockRequests.getFirst().txn.getTransactionId() + "current wait reduced by 1 " + locksAcquired.get(lockRequests.getFirst().txn.getTransactionId()).needed );
//            }
        }else{
            while(it.hasNext()){
                LockRequest curr = it.next();
                if(isShared &&  LockMode.EXCLUSIVE.equals(curr.mode))
                    break;
                else if(!curr.txn.getTransactionId().equals(txn.getTransactionId())){
                    initializeLockAcquired(curr.txn);
                    addLockGranted(curr.txn.getTransactionId());
                }
            }
        }
//        System.out.println("Released lock for transaction : " + txn.getTransactionId() + " Key :"  + key+ " Partition :" + partitionId);

    }

//    public boolean isTransactionReady(Transaction txn){
//        return txn_waits.get(txn) == null || txn_waits.get(txn) <= 0;
//    }

    synchronized public boolean isLockedByMe(int key, String id){
        List<String> owners = new ArrayList<>();
        Deque<LockRequest> lockRequests = lockTable.get(key);
        for (LockRequest curr : lockRequests) {
            if (curr.mode == LockMode.SHARED) {
                owners.add(curr.txn.getTransactionId());
            } else if (curr.mode == LockMode.EXCLUSIVE && owners.isEmpty()) {
                owners.add(curr.txn.getTransactionId());
                return true;
            } else {
                return true;
            }
        }
        return owners.contains(id);
    }

    synchronized public void addToReadyQueue(Transaction txn){
        for(Map.Entry<Integer,ArrayBlockingQueue<Transaction>> entry : ready_txns.entrySet()) {
            try {
                entry.getValue().put(txn);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

//    public synchronized void addToCompleted(String id){
//        completedTransactions.put(id,true);
//    }


    public ArrayBlockingQueue<Transaction> getReady_txns(int no) {
        return ready_txns.get(no);
    }


}