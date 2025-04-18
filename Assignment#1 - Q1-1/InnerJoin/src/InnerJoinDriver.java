import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InnerJoinDriver extends Configured implements Tool {
	@Override
	public int run(String args[]) throws Exception {
		if(args.length < 2){
			System.out.println("PLease give valid inputs");
			return -1;
		}
		JobConf conf = new JobConf(InnerJoinDriver.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setMapperClass(JoinMapper.class);
        conf.setReducerClass(JoinReducer.class);;
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		JobClient.runJob(conf);
		return 0;
	}
	 public static void main(String args[]) throws Exception {
	        int exitCode = ToolRunner.run(new InnerJoinDriver(), args);
	        System.out.println(exitCode);
	    }

}
