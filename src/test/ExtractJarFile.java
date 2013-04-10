package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ExtractJarFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String jarfile = "testjar.jar";
		String destdir = "bin";
		
		try {
			JarFile jar = new JarFile(jarfile);
			Enumeration enums = jar.entries();
			
			while (enums.hasMoreElements()) {
				JarEntry file = (JarEntry) enums.nextElement();
				
				System.out.println(destdir + File.separator + file.getName());
				File f = new File(destdir + File.separator + file.getName());
				if (f.isDirectory()) {
					f.mkdirs();
					continue;
				}
				
				InputStream is = jar.getInputStream(file);

				FileOutputStream fos = null;
				
				try {
					fos = new FileOutputStream(f);
				} catch (FileNotFoundException e) {
					f.getParentFile().mkdirs();
					fos = new FileOutputStream(f);
				}
				
				while(is.available() > 0) {
					fos.write(is.read());
				}
				
				fos.close();
				is.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			Class c = ExtractJarFile.class.getClassLoader().loadClass("TestClass");
			Method method = c.getMethod("helloworld", null);
			Object t = c.newInstance();
			method.invoke(t, null);
		} catch (ClassNotFoundException e) {
			System.out.println("Class Not Found");
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
