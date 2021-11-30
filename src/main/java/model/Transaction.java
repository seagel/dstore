package model;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

public class Transaction implements Serializable {

    /**
     * readSet --> will contain the list of values to be read
     * writeSet --> will contain the list of values to be written
     * isMultipartition -> whether a transaction involves multiple partition.
     */
    String tId;
    private static final long serialVersionUID = 4435454164550314194L;
    Set<Integer> readSet;
    Set<Integer> writeSet;
    boolean isMultiPartition;



    int originatorPartition;

    public Transaction(Set<Integer> readSet,Set<Integer> writeSet,boolean isMultiPartition){
        super();
        this.readSet = readSet;
        this.writeSet = writeSet;
        this.isMultiPartition = isMultiPartition;
    }


    //getter & setter methods

    public Set<Integer> getReadSet()
    {
        return this.readSet;
    }

    public Set<Integer> getWriteSet()
    {
        return this.writeSet;
    }

    public String getTransactionId()
    {
        return this.tId;
    }
    public boolean isMultiPartition() {
        return isMultiPartition;
    }
    public void settId(String tId) {
        this.tId = tId;
    }
    public void setOriginatorPartition(int originatorPartition) {
        this.originatorPartition = originatorPartition;
    }

    public void print(){
        StringBuilder ans = new StringBuilder();
        for(Integer x : readSet){
            ans.append(x.toString());
        }
        for(Integer x : writeSet){
            ans.append(x.toString());
        }
        ans.append("Partition id:");
        ans.append(tId);
        System.out.println(ans);
    }

//    @Override
//    public void serialize(OutputArchive outputArchive, String s) throws IOException {
//        outputArchive.startRecord(this, s);
//        ByteBuffer readBuffer = ByteBuffer.allocate(readSet.size());
//        for (Integer key : readSet) {
//            readBuffer.put(key.byteValue());
//        }
//        outputArchive.writeBuffer(readBuffer.array(), "readSet");
//        ByteBuffer writeBuffer = ByteBuffer.allocate(writeSet.size());
//        for (Integer key : writeSet) {
//            writeBuffer.put(key.byteValue());
//        }
//        outputArchive.writeBuffer(writeBuffer.array(), "writeset");
//        outputArchive.writeBool(isMultiPartition, "isMultipartition");
//        outputArchive.endRecord(this,s);
//    }
//
//    @Override
//    public void deserialize(InputArchive inputArchive, String s) throws IOException {
//        inputArchive.startRecord(s);
//        this.isMultiPartition = inputArchive.readBool("isMultipartition");
//        byte[] readSetBuffer = inputArchive.readBuffer("readSet");
//        byte[] writeset
//        inputArchive.endRecord(s);
//    }
}


