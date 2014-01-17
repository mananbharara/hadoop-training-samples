package com.thoughtworks.samples.hadoop.mapred.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LexicalPartitioner extends Partitioner<Text, IntWritable> {

  @Override
  public int getPartition(Text word, IntWritable count, int numReducers) {
    String s = word.toString();
    if (s.length() == 0) {
      return 0;
    }
    if (word.toString().equals("hotmail.com")) {
      return 0;
    }

    return (new Character(word.toString().charAt(0)).hashCode() % (numReducers -1)) + 1;
    }
}
