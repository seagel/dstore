package Tests;
import model.Transaction;

public class GenerateTransactions {

	Set<Integer> readset = new HashSet<>();
	Set<Integer> writeset = new HashSet<>();

	public Transaction genTrans(int length;int contention){

		Random random = new Random();
		if(contention == 0){
			for(int i=0; i<=length; i++){
				int read = (random.nextInt()) % DATABASE_SIZE;
				readset.add(read);
			}

			for(int i=0; i<=length; i++){
				int write = (random.nextInt()) % DATABASE_SIZE;
				writeset.add(write);
			}

		}
		else if(contention == 1){
			for(int i =0; i<=length; i++){
				int read = (random.nextInt()) % 100;
				readset.add(read);
			}
			for(int i=0; i<=length; i++){
				int write = (random.nextInt()) % 100;
				writeset.add(write);
			}
		}
		return new Transaction(readset,writeset,true);
	}

	public List<Transaction> listoftxns(int length;int contention){
		for(int i=0; i<=PROCESSING_QUEUE_SIZE; i++){
			List<Transaction> transactionList = new List<Transaction>();
			transactionList.add(genTrans(int length;int contention));
		}
	}

	String type;


	

}
