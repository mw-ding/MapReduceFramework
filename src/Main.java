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
    jobConf.setBlockSize(10);
    jobConf.setJobName("test");
    jobConf.setMapperClassName("TestMapper");
    jobConf.setReducerClassName("TestReducer");
    jobConf.setInputFormatClassName("MyInputFormat");
    jobConf.setOutputFormatClassName("MyOutputFormat");
    jobConf.setPartitionerClassName("Partitioner");
    jobConf.setOutputPath("config");
    jobConf.setReducerNum(3);
    Job job = new Job(jobConf);
    job.run();
  }
}
