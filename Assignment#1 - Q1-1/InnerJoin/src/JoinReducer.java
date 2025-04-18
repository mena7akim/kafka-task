import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;



public class JoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private Text outputValue = new Text();
	private Text emptyText = new Text("");
	
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
		String t1Value = null;
		String t2Value = null;
		while (values.hasNext()){
			Text val = values.next();
			String[] parts = val.toString().split(",");
			if(parts[0].equals("T1")){
				t1Value = parts[1];
			}
			else if(parts[0].equals("T2")){
				t2Value = parts[1];
			}
		}
		if(t1Value != null && t2Value != null){
			outputValue.set(t2Value + "," + key.toString() + "," + t1Value);
			output.collect(emptyText, outputValue);
			
		}
	}

}
