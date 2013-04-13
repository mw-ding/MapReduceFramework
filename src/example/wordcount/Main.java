package example.wordcount;

import mapreduce.*;

public class Main {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage: wordcount <input_path> <output_path>");
      return ;
    }
    
    JobConf jconf = new JobConf();
    jconf.setJobName("WordCount");
    
    jconf.setInputPath(args[0]);
    jconf.setOutputPath(args[1]);
    
    jconf.setBlockSize(1000);
    
    jconf.setMapperClassName("example.wordcount.WCMapper");
    jconf.setReducerClassName("example.wordcount.WCReducer");
    
    jconf.setInputFormatClassName("example.wordcount.WCInputFormat");
    jconf.setOutputFormatClassName("example.wordcount.WCOutputFormat");
    jconf.setPartitionerClassName("example.wordcount.WCPartitioner");
    
    jconf.setReducerNum(4);
    
    Job job = new Job(jconf);
    job.run();
  }

}
