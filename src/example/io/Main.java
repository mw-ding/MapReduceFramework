package example.io;

import java.io.IOException;

import mapreduce.Job;
import mapreduce.JobConf;

public class Main {

  public static void main(String[] args) {
    JobConf jobConf = new JobConf();
    jobConf.setInputPath("config/config");
    jobConf.setBlockSize(10);
    jobConf.setJobName("test");
    jobConf.setMapperClassName("TestMapper");
    jobConf.setReducerClassName("TestReducer");
    jobConf.setInputFormatClassName("MyInputFormat");
    jobConf.setOutputFormatClassName("MyOutputFormat");
    jobConf.setPartitionerClassName("Partitioner");
    jobConf.setOutputPath("testoutput" + Math.random());
    jobConf.setReducerNum(3);
    Job job = new Job(jobConf);
    job.run();
  }
}
