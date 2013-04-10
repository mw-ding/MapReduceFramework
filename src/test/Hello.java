package test;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Hello extends UnicastRemoteObject implements HelloInterface {
	
	private String message;
	public Hello(String msg) throws RemoteException {
		this.message = msg;
	}
	
	public String say() throws RemoteException {
		return this.message;
	}
	
	public static void main(String[] args){
	  /* periodically send status progress to job tracker */
    ScheduledExecutorService schExec = Executors.newScheduledThreadPool(8);
    ScheduledFuture<?> schFuture = schExec.scheduleAtFixedRate(new Runnable() {
      public void run() {
        System.out.println("run");
      }
    }, 0, 2, TimeUnit.SECONDS);
    System.out.println("main");
	}
	
}
