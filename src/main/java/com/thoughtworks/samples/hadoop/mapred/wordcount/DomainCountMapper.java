package com.thoughtworks.samples.hadoop.mapred.wordcount;

import com.thoughtworks.samples.hadoop.mapred.domaincount.DOMAIN;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DomainCountMapper extends Mapper<Object, Text, Text, IntWritable> {

  @Override
  public void map(Object key, Text text, Mapper.Context context) throws IOException, InterruptedException {
    String domain = text.toString().split("@")[1];

    if(ArrayUtils.contains(context.getConfiguration().getStrings("wordcount.ignored.domains"), domain)) {
      context.getCounter(DOMAIN.HOTMAIL_DOMAIN).increment(1);
      return;
    }

    context.write(new Text(domain), new IntWritable(1));
    context.getCounter(DOMAIN.OTHER_DOMAIN).increment(1);
  }
}
