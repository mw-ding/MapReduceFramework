import java.io.IOException;

public class Main {

  public static void main(String[] args) {

//    try {
//      Utility.startProcess(new String[] { "rmiregistry", "12345" });
//      Utility.startJavaProcess(new String[] { TaskTracker.class.getName(), "1" });
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
    JobConf jobConf = new JobConf();
    jobConf.setInputPath("config/config");
    jobConf.setBlockSize(5);
    jobConf.setJobName("test");
    jobConf.setMapperClassName("Mapper");
    jobConf.setOutputPath("");
    jobConf.setReducerClassName("Reducer");
    jobConf.setReducerNum(3);
    Job job = new Job(jobConf);
    job.run();
  }
}
