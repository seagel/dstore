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
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(30,80,1,0));
        partition.addToProduceQueue(collection);
    }
    
    
    @Test
    public void test2()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(30,50,0,0));
        partition.addToProduceQueue(collection);
    }
    
    @Test
    public void test3()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(5,80,1,0));
        partition.addToProduceQueue(collection);
    }
	
    @Test
    public void test4()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(5,50,0,1));
        partition.addToProduceQueue(collection);
    }
    
    @Test
    public void test5()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(100,80,1,1));
        partition.addToProduceQueue(collection);
    }
    
    @Test
    public void test6()
    {
    	Collection<Transaction> collection = new ArrayList<Transaction>(generateTransactions.listoftxns(100,50,0,1));
        partition.addToProduceQueue(collection);
    }

}
