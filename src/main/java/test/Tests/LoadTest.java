package test.Tests;

import java.util.List;

import model.Transaction;


public class LoadTest {

    public static List<Transaction> mixedHighContention() {
        return GenerateTransactions.listoftxns(30, 80, 1);
    }

    public static List<Transaction> readWriteOnlyLowContention() {
        return GenerateTransactions.listoftxns(1000, 0, 0);
    }

    public static List<Transaction> readOnlyHighContention() {
        return GenerateTransactions.listoftxns(30, 100, 1);
    }


    public static List<Transaction> readOnlyLowContention() {
        return GenerateTransactions.listoftxns(30, 100, 0);
    }


}
