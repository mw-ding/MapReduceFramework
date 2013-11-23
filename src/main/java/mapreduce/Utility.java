package mapreduce;

import java.io.File;

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
    newargs[2] = classpath + File.pathSeparator + Constants.getResource(Constants.USER_CLASS_PATH) + separator + "job" + jid;

    /* get the rmi codebase path */
    newargs[3] = "-Djava.rmi.server.codebase=file:" + Constants.getResource(Constants.RMI_CODE_BASE);
    for (int i = 4, j = 0; j < args.length; i++, j++) {
      newargs[i] = args[j];
    }

    /* start a process with above arguments */
    ProcessBuilder processBuilder = new ProcessBuilder(newargs);
    processBuilder.start();
  }
}
