package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * 
 * This is a utility class. methods included are: 
 * 1. String getParam(String key) 
 * 2. void startJavaProcess(String[] args, int jid) throws Exception 
 * 3. void startProcess(String[] args) throws Exception
 */
public class Utility {

  /* MAPREDUCE_HOME used to locate config file */
  private static String MAPREDUCE_HOME = System.getenv().get("MAPREDUCE_HOME");

  /**
   * method used to read config information from config file
   * 
   * @param key
   *          of config info
   * @return value of config info
   */
  public static String getParam(String key) {

    /* locate the path of config file */
    String configPath = MAPREDUCE_HOME + "/config/config";
    FileInputStream fis = null;
    BufferedReader br = null;
    try {
      fis = new FileInputStream(configPath);
      br = new BufferedReader(new InputStreamReader(fis));
      String line = "";
      while ((line = br.readLine()) != null) {
        int ind = line.indexOf('=');
        String k = line.substring(0, ind);
        if (k.equals(key)) {
          String s = line.substring(ind + 1);
          return line.substring(ind + 1);
        }
      }
      throw new RuntimeException("cannot find param " + key);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null && fis != null)
        try {
          br.close();
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
    }
    return "";
  }

  /**
   * method used to start of new java process
   * 
   * @param args
   * @param jid
   * @throws Exception
   */
  public static void startJavaProcess(String[] args, int jid) throws Exception {
    /* build arguments */
    String separator = System.getProperty("file.separator");
    String classpath = System.getProperty("java.class.path");
    String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
    String[] newargs = new String[args.length + 4]; /* three more args for path, -cp, classpath */
    newargs[0] = path;
    newargs[1] = "-cp";
    newargs[2] = classpath + File.pathSeparator + classpath + separator + "job" + jid;
    System.out.println("CLASSPATH: " + newargs[2]);

    /* get the rmi codebase path */
    newargs[3] = "-Djava.rmi.server.codebase=file:" + Utility.getParam("RMI_CODE_BASE");
    for (int i = 4, j = 0; j < args.length; i++, j++) {
      newargs[i] = args[j];
    }

    /* start a process with above arguments */
    ProcessBuilder processBuilder = new ProcessBuilder(newargs);
    processBuilder.start();
  }

  /**
   * method used to start a general process
   * 
   * @param args
   * @throws Exception
   */
  public static void startProcess(String[] args) throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder(args);
    processBuilder.start();
  }
}
