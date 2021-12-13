package test.Tests;

import java.util.ArrayList;
import java.util.List;

import model.Transaction;
import partition.Range;


/**
 * 10,20,30 - Partitions
 * 100% MH 0%SH  (80R 20RW) (60R 40RW) (high contention and low contention)  - test1 & test2 & test3 & test4
 * 20% MH 80% SH (80R 20RW) (60R 40RW)  (high contention and low contention) - test5 & test 6 & test7 & test8
 * 100% SH (80R 20RW) (60R 40RW)  (high contention and low contention) - test9 &test10 & test11 & test12
 */
public class LoadTest {

    GenerateTransactions generateTransactions;
    public LoadTest(ArrayList<Range> ranges) {
        generateTransactions = new GenerateTransactions(ranges);
    }

    public  List<Transaction> test1() {
        return generateTransactions.listoftxns(30, 0.8f, 1,1);
    }

    public  List<Transaction> test2() {
        return generateTransactions.listoftxns(30, 0.6f, 1,1);
    }

    public  List<Transaction> test3() {
        return generateTransactions.listoftxns(30, 0.8f, 0,1);
    }

    public  List<Transaction> test4() {
        return generateTransactions.listoftxns(30, 0.6f, 0,1);
    }

    public  List<Transaction> test5() {
        return generateTransactions.listoftxns(30, 0.8f, 1,0.1f);
    }


    public List<Transaction> test6(int home) {
        return generateTransactions.listSHTxns(30, 0.8f, 1,home);

    }
}
