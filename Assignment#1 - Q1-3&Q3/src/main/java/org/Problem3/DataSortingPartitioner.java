package org.Problem3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class DataSortingPartitioner implements Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        int letter = text.toString().charAt(0) - 'a' + 1;
        int partition = (int)Math.ceil((letter * i) / 26.0);
        return partition - 1;
    }

    @Override
    public void configure(JobConf jobConf) {
        // No configuration needed for this partitioner
    }
}