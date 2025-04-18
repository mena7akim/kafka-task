import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class JoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override 
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
		String line =value.toString().trim();
		String[] parts = line.replace("(","").replace(")","").split(",\\s");
		if(parts.length == 3){
			String tableName = parts[0];
			String pk = parts[1];
			String attributeValue  = parts[2];
			if(tableName.equals("T1")){
				outputKey.set(pk);
				outputValue.set("T1," + attributeValue );
				output.collect(outputKey, outputValue);
			}
			else if(tableName.equals("T2")){
				outputKey.set(attributeValue);
				outputValue.set("T2," + pk);
				output.collect(outputKey, outputValue);
			}
			
		}
		
	}

}
