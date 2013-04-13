package example.io;

import java.io.IOException;

import mapreduce.Job;
import mapreduce.JobConf;

public class Main {

  public static void main(String[] args) {
    if (args.length != 3)
      return ;
    
    JobConf jobConf = new JobConf();
    jobConf.setJarFilePath(args[2]);
    jobConf.setInputPath(args[0]);
    jobConf.setBlockSize(64);
    jobConf.setJobName("test");
    jobConf.setMapperClassName("example.io.TestMapper");
    jobConf.setReducerClassName("example.io.TestReducer");
    jobConf.setInputFormatClassName("example.io.MyInputFormat");
    jobConf.setOutputFormatClassName("example.io.MyOutputFormat");
    jobConf.setPartitionerClassName("example.io.MyPartitioner");
    jobConf.setOutputPath(args[1]);
    jobConf.setReducerNum(3);
    Job job = new Job(jobConf);
    job.run();
  }
}
