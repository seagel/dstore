package storage;

import model.Transaction;

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
public class LockManager {
    private final ConcurrentHashMap<Integer, Deque<LockRequest>> lockTable ;
    private final Map<String,Integer> txn_waits;



    Map<Integer,ArrayBlockingQueue<Transaction>> ready_txns;

    public LockManager(Map<Integer,ArrayBlockingQueue<Transaction>> ready_txns){
        lockTable = new ConcurrentHashMap<>();
        txn_waits = new ConcurrentHashMap<>();
        this.ready_txns = ready_txns;
    }
    synchronized public boolean writeLock(Transaction txn,int key,int partitionId){
        boolean isLockGranted = false;
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests == null || lockRequests.isEmpty()){
            isLockGranted = true;
        }else{
            txn_waits.putIfAbsent(txn.getTransactionId(), 0);
            txn_waits.merge(txn.getTransactionId(), 1, Integer::sum);
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

    synchronized public boolean readLock(Transaction txn,int key,int partitionId){
        boolean isLockGranted = false;
        Deque<LockRequest> lockRequests = lockTable.get(key);
        if(lockRequests == null || lockRequests.isEmpty()){
            isLockGranted = true;
        }
        synchronized (this){
            LockRequest lockRequest = new LockRequest(txn, LockMode.SHARED,partitionId);
            Deque<LockRequest> temp = new ConcurrentLinkedDeque<>();
            if (lockRequests == null) {
                temp.addLast(lockRequest);
                lockTable.put(key, temp);
            } else {
                lockRequests.add(lockRequest);
            }
        }
        lockRequests = lockTable.get(key);
        Iterator<LockRequest> it = lockRequests.iterator();
        boolean isExclusiveLock = it.next().mode == LockMode.EXCLUSIVE;
        if(!isLockGranted && !isExclusiveLock){
            isLockGranted = true;
            while(it.hasNext()){
                if(it.next().mode != LockMode.SHARED){
                    isLockGranted = false;
                    break;
                }
            }
        }
        if(!isLockGranted){
            txn_waits.putIfAbsent(txn.getTransactionId(), 0);
            txn_waits.merge(txn.getTransactionId(), 1, Integer::sum);
        }
//        System.out.println("Read lock for transaction : " + txn.getTransactionId() + " Granted : " + isLockGranted + " key :" + key + " Partition :" + partitionId);
        return isLockGranted;
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
        boolean isShared = lockRequests.getFirst().mode == LockMode.SHARED;

        if(!isShared && lockRequests.getFirst().txn != txn){
//            System.out.println("Transaction : " + lockRequests.getFirst().txn.getTransactionId() + "wait : " + txn_waits.get(lockRequests.getFirst().txn.getTransactionId()));
            txn_waits.merge(lockRequests.getFirst().txn.getTransactionId(),-1,Integer::sum);
//            System.out.println("Transaction : " + lockRequests.getFirst().txn.getTransactionId() + "wait : " + txn_waits.get(lockRequests.getFirst().txn.getTransactionId()));
            if(txn_waits.get(lockRequests.getFirst().txn.getTransactionId()) == 0){
                addToReadyQueue(lockRequests.getFirst().txn);
//                System.out.println("Adding to ready queue :" + lockRequests.getFirst().txn.getTransactionId());
            }
        }else{
            while(it.hasNext()){
                LockRequest curr = it.next();
                if(isShared && curr.mode == LockMode.EXCLUSIVE)
                    break;
                else if(curr.txn.getTransactionId().equals(txn.getTransactionId())){
                    txn_waits.merge(txn.getTransactionId(),-1,Integer::sum);
                    if( txn_waits.get(curr.txn.getTransactionId()) <= 0)
                        addToReadyQueue(curr.txn);
                }
            }
        }
//        System.out.println("Released lock for transaction : " + txn.getTransactionId() + " Key :"  + key+ " Partition :" + partitionId);

    }

//    public boolean isTransactionReady(Transaction txn){
//        return txn_waits.get(txn) == null || txn_waits.get(txn) <= 0;
//    }

    synchronized public LockMode status(int key, List<Transaction> owners){
        Deque<LockRequest> lockRequests = lockTable.get(key);
        owners.clear();
        for (LockRequest curr : lockRequests) {
            if (curr.mode == LockMode.SHARED) {
                owners.add(curr.txn);
            } else if (curr.mode == LockMode.EXCLUSIVE && owners.isEmpty()) {
                owners.add(curr.txn);
                return LockMode.EXCLUSIVE;
            } else {
                return LockMode.SHARED;
            }
        }
        return owners.isEmpty() ? LockMode.UNLOCKED : LockMode.SHARED;
    }

    public void addToReadyQueue(Transaction txn){
        for(Map.Entry<Integer,ArrayBlockingQueue<Transaction>> entry : ready_txns.entrySet()) {
            try {
                entry.getValue().put(txn);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public boolean isCompleted(){
        boolean isCompleted = true;
        for(Map.Entry<String,Integer> entry : this.txn_waits.entrySet()){
            if(entry.getValue() > 0){
//                System.out.println(entry.getKey());
                isCompleted = false;
                break;
            }
        }
        return isCompleted;
    }

    public ArrayBlockingQueue<Transaction> getReady_txns(int no) {
        return ready_txns.get(no);
    }


}