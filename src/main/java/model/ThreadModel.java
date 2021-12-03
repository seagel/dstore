package model;

public class ThreadModel implements Runnable {
	String name;
	int partition;
	int last_read_transaction;
	Thread t;
	
	public ThreadModel(String threadName)
	{	
		this.name = threadName; 
	    t = new Thread(this, name);
	    System.out.println("New thread: " + t);
	    t.start();
	}
	
	public void run() // mention the readOperations and Write Operations inside this, this is what will run when thread starts
    {
        try {
            System.out.println("Thread " + this.t.currentThread().getId()+ " is running");
        }
        catch (Exception e) {
            System.out.println("Exception is caught");
        }
    }
	
	

}