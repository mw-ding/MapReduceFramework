package test;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Rmitestclient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Registry register = LocateRegistry.getRegistry();
			HelloInterface stub = (HelloInterface) register.lookup("hello");
			String response = stub.say();
			System.out.println(response);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

}
