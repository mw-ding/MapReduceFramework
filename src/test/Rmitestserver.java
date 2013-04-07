package test;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Rmitestserver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			HelloInterface hi = (HelloInterface) new Hello("Hello World");
			
			Registry reg = LocateRegistry.getRegistry();
			reg.rebind("hello", hi);
			reg.rebind("hello1", hi);
			for (String s : reg.list()) {
				System.out.println(s);
			}
			System.out.println("Hello Server is ready.");
			while(true);
		} catch (Exception e) {
			System.out.println ("Hello server failed: " + e);
		}
	}

}
