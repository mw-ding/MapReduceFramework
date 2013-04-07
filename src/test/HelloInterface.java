package test;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface HelloInterface extends Remote {
	
	public String say() throws RemoteException;
}
