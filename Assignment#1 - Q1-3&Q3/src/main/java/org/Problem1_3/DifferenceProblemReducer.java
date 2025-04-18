package org.Problem1_3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class DifferenceProblemReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    private final static Text emptyText = new Text("");

    @Override
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
        int len = 0;
        String tableName = "";
        while (values.hasNext()) {
            len++;
            tableName = values.next().toString();
        }
        if(len == 1 && tableName.equals("T1")){
            outputCollector.collect(key, emptyText);
        }
    }
}