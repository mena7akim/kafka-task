import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendsOfFriends {
    
    public static class FriendsOfFriendsMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            
            if (parts.length == 3) {
                String p1 = parts[0];
                String p2 = parts[1];
                String weight = parts[2];
                
                String person = context.getConfiguration().get("targetPerson");
                
                if (p1.equals(person)) {
                    outputKey.set(p2);
                    outputValue.set(p1 + "," + weight + ",1st");
                    context.write(outputKey, outputValue);
                    return;
                }
                
                if (p2.equals(person)) {
                    outputKey.set(p1);
                    outputValue.set(p2 + "," + weight + ",1st");
                    context.write(outputKey, outputValue);
                    return;
                }
                
                outputKey.set(p1);
                outputValue.set(p2 + "," + weight + ",others");
                context.write(outputKey, outputValue);
                
                outputKey.set(p2);
                outputValue.set(p1 + "," + weight + ",others");
                context.write(outputKey, outputValue);
            }
        }
    }
    
    public static class FriendsOfFriendsReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private Text outputKey = new Text();
        private DoubleWritable outputValue = new DoubleWritable();
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String[]> allValues = new ArrayList();
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                allValues.add(new String[]{parts[0], parts[1], parts[2]});
            }
            
            String firstNode = null;
            Double firstWeight = null;
            
            for (String[] entry : allValues) {
                String p2 = entry[0];
                String weight = entry[1];
                String flag = entry[2];
                
                if (flag.equals("1st")) {
                    firstWeight = Double.parseDouble(weight);
                    firstNode = p2;
                    break;
                }
            }
            
            if (firstNode != null && firstWeight != null) {
                for (String[] entry : allValues) {
                    String p2 = entry[0];
                    String weight = entry[1];
                    String flag = entry[2];
                    
                    if (flag.equals("1st")) {
                        continue;
                    }
                    
                    String pathDescription = firstNode + " -> " + key.toString() + " -> " + p2;
                    double totalWeight = firstWeight + Double.parseDouble(weight);
                    
                    outputKey.set(pathDescription);
                    outputValue.set(totalWeight);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: FriendsOfFriendsExact <input path> <output path> <target person>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        conf.set("targetPerson", args[2]);
        
        Job job = Job.getInstance(conf, "Friends of Friends Exact");
        job.setJarByClass(FriendsOfFriends.class);
        job.setMapperClass(FriendsOfFriendsMapper.class);
        job.setReducerClass(FriendsOfFriendsReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}