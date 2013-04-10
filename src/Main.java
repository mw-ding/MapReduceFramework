import java.io.IOException;

public class Main {

  public static void main(String[] args) {

    try {
      Utility.startProcess(new String[] { "rmiregistry", "12345" });
      Utility.startJavaProcess(new String[] { TaskTracker.class.getName(), "1" });
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
