import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OuterJoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String record = value.toString();
        record = record.substring(1, record.length()-1);
        String[] elements = record.split(",");
        String tableName = elements[0].trim(), pk = elements[1].trim(), attribute = elements[2].trim();
        
        if (tableName.equals("T1")) {
        	output.collect(new Text(pk), new Text(tableName + ":" + attribute));
        }
        else if (tableName.equals("T2")) {
        	output.collect(new Text(attribute), new Text(tableName + ":" + pk));
        }
    }
}