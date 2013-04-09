package test;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Rmitestclient {
	
	static StringBuilder str = new StringBuilder();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StringBuilder str1 = str;
		StringBuilder str2 = str;
		
		synchronized(str1) {
			synchronized(str2) {
			str1.append("hello");
			System.out.println(str1.toString());
			}
		}
//		try {
//			Registry register = LocateRegistry.getRegistry();
//			HelloInterface stub = (HelloInterface) register.lookup("hello");
//			String response = stub.say();
//			System.out.println(response);
//		} catch (RemoteException e) {
//			e.printStackTrace();
//		} catch (NotBoundException e) {
//			e.printStackTrace();
//		}
	}

}
