package org.Problem1_3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class DifferenceProblemMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
    private final static IntWritable key = new IntWritable();
    private final static Text value = new Text();

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
        String line = text.toString();
        line = line.replace("(", "");
        line = line.replace(")", "");
        String[] data = line.split(", ");
        String tableName = data[0];
        String A1;
        if (tableName.equals("T1")) {
            A1 = data[1];
        } else {
            A1 = data[2];
        }
        key.set(Integer.parseInt(A1));
        value.set(tableName);
        outputCollector.collect(key, value);
    }
}
