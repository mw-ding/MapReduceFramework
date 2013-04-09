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
    String configPath = MAPREDUCE_HOME + "/config";
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
      throw new RuntimeException("cannot find param "+key);
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
}
