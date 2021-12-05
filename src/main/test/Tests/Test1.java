package Tests;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.jupiter.api.Test;

import model.Transaction;
import partition.Initializer;
import partition.Partition;
import partition.Range;
import zookeeper.Executor;

public class Test1 {
	Range r = new Range(-1,-2);
	Executor e = new Executor("localhost");
	Initializer i = new Initializer(e);
	Partition partition = new Partition(123,i,r);
	GenerateTransactions generateTransactions = new GenerateTransactions();

    @Test
    public void test1()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(30,1));
        partition.addToProduceQueue(collection);
    }
    
    
    @Test
    public void test2()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(30,0));
        partition.addToProduceQueue(collection);
    }
    
    @Test
    public void test3()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(5,1));
        partition.addToProduceQueue(collection);
    }
	
    @Test
    public void test4()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(5,0));
        partition.addToProduceQueue(collection);
    }
    
    @Test
    public void test5()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(100,1));
        partition.addToProduceQueue(collection);
    }
    
    @Test
    public void test6()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(100,0));
        partition.addToProduceQueue(collection);
    }

}
