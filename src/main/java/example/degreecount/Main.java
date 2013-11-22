package example.degreecount;

import mapreduce.Job;
import mapreduce.JobConf;

/**
 * This example code is to count the in-degree and out-degree of vertices in graph. The input is the
 * edges in the graph, with the first column as starting vertex id and the second column as the
 * ending vertex id. The columns are separated by tab. The output is the in-degree or out-degree of
 * each vertices. The first column is the vertex id. The second column is the in-degree. The third
 * column is the out-degree.
 * 
 * It requires two arguments: input path and output path.
 * */
public class Main {
  public static void main(String[] args) {
    if (args.length != 3)
      return;
    String input = args[0];
    String output = args[1];
    JobConf jobConf = new JobConf();
    jobConf.setJarFilePath(args[2]);
    jobConf.setInputPath(input);
    jobConf.setBlockSize(100000);
    jobConf.setJobName("degree count");
    jobConf.setMapperClassName("example.degreecount.DegreeCountMapper");
    jobConf.setReducerClassName("example.degreecount.DegreeCountReducer");
    jobConf.setInputFormatClassName("example.degreecount.MyInputFormat");
    jobConf.setOutputFormatClassName("example.degreecount.MyOutputFormat");
    jobConf.setPartitionerClassName("example.degreecount.MyPartitioner");
    jobConf.setOutputPath(output);
    jobConf.setReducerNum(3);
    Job job = new Job(jobConf);
    job.run();
  }
}
