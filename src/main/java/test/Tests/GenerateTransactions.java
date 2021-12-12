package test.Tests;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import constants.ConfigurableConstants;
import model.Transaction;

public class GenerateTransactions {

	
	/**
	 * 0 - Read-only mode
	 * 1 - Read-Write mode
	 */
	public static Transaction genTrans(int length, float percentageRead, float contention)
	{
		Set<Integer> readSet = new HashSet<>();
		Set<Integer> writeSet = new HashSet<>();
		
		Random random = new Random();
		int lengthRead = (int) (percentageRead*length/100);
		int lengthReadWrite = (int)((1-percentageRead)*length/100);
		for(int i=0; i< lengthRead; i++)
		{
			int read = (int) (Math.abs(random.nextInt()) % (100 + (1000 * (1 - contention))));
			readSet.add(read);
		}
			for(int i=0; i<lengthReadWrite; i++)
			{
				int write = (int) (Math.abs(random.nextInt()) % (1000 * (1 - contention)));
				writeSet.add(write);
			}
		return new Transaction(readSet,writeSet,true);
	}

	public static List<Transaction> listoftxns(int length, float percentageRead, float contention)
	{
		List<Transaction> transactionList = new ArrayList<>();
		for(int i=0; i<ConfigurableConstants.TRANSACTION_LENGTH; i++)
		{
			transactionList.add(genTrans(length,percentageRead,contention));
		}
		return transactionList;
	}


}
