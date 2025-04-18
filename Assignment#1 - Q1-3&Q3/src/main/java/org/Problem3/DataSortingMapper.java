package org.Problem3;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;
import java.io.IOException;

public class DataSortingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    final private Text word = new Text();
    final private IntWritable value = new IntWritable(1);
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String[] line = text.toString().split(" ");
        if(line.length == 2){
            value.set(Integer.parseInt(line[1]));
            word.set(line[0]);
            outputCollector.collect(word, value);
        }
    }
}
