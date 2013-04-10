import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class Utility {
  private static String MAPREDUCE_HOME = System.getenv().get("MAPREDUCE_HOME");

  public static String getParam(String key) {
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
          return line.substring(ind + 1);
        }
      }
      throw new RuntimeException("cannot find param " + key);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        br.close();
        fis.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return "";
  }

  public static void startJavaProcess(String[] args) throws Exception {
    String separator = System.getProperty("file.separator");
    String classpath = System.getProperty("java.class.path");
    String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
    String[] newargs = new String[args.length + 3]; /* three more args for path, -cp, classpath */
    newargs[0] = path;
    newargs[1] = "-cp";
    newargs[2] = classpath;
    for (int i = 3, j = 0; j < args.length; i++, j++) {
      newargs[i] = args[j];
    }
    ProcessBuilder processBuilder = new ProcessBuilder(newargs);
    Process process = processBuilder.start();
  }
  
  public static void startProcess(String[] args) throws Exception{
    ProcessBuilder processBuilder = new ProcessBuilder(args);
    Process process = processBuilder.start();
  }
}
