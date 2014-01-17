package com.thoughtworks.samples.hadoop.mapred.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DomainCountMapper extends Mapper<Object, Text, Text, IntWritable>  {

  @Override
  public void map(Object key, Text text, Mapper.Context context) throws IOException, InterruptedException {
    String line = text.toString();
    String[] tokens = line.split(" ");
    for (String token : tokens) {
      String domain = token.split("@")[1];
      context.write(new Text(domain), new IntWritable(1));
    }
  }
}
