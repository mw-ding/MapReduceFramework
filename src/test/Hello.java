package test;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class Hello extends UnicastRemoteObject implements HelloInterface {
	
	private String message;
	public Hello(String msg) throws RemoteException {
		this.message = msg;
	}
	
	public String say() throws RemoteException {
		return this.message;
	}
	
}
