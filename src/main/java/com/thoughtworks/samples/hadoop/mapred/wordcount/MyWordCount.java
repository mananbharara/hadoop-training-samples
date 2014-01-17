package com.thoughtworks.samples.hadoop.mapred.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyWordCount extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MyWordCount(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job wordCountJob = new Job(conf, "MyWordCount");
    wordCountJob.setJarByClass(MyWordCount.class);
    wordCountJob.setMapperClass(DomainCountMapper.class);
    wordCountJob.setReducerClass(WCReducer.class);
    wordCountJob.setOutputKeyClass(Text.class);
    wordCountJob.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
    wordCountJob.setNumReduceTasks(4);
    wordCountJob.waitForCompletion(true);
    return 0;
  }
}
