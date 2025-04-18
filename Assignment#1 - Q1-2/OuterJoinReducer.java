import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class OuterJoinReducer extends MapReduceBase implements Reducer <Text, Text, Text, Text>{
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter rep) throws IOException{
		List<String> records1 = new ArrayList<>();
        List<String> records2 = new ArrayList<>();
        
        while (values.hasNext()) {
            String record = values.next().toString();
            int colon = record.indexOf(':');
            String tableName = record.substring(0, colon);
            String attribute = record.substring(colon+1);
            
            if (tableName.equals("T1")) {
                records1.add(attribute);
            } else{
                records2.add(attribute); 
            }
        }
        
        // (PK2, Join Key, attribute1)
        if (!records1.isEmpty() && !records2.isEmpty()) {
            for (String record1 : records1) {
                for (String record2 : records2) {
                    output.collect(key, new Text("(" + record2 + ", " + key.toString() + ", " + record1 + ")"));
                }
            }
        } 
        // (null, pk1, attribute1)
        else if (!records1.isEmpty()) {
            for (String record1 : records1) {
                output.collect(key, new Text("(null, " + key.toString() + ", " + record1 + ")"));
            }
        }
        // (pk2, null, null)
        else if (!records2.isEmpty()) {
            for (String record2 : records2) {
                output.collect(key, new Text("(" + record2 + ", null, null)"));
            }
        }
	}
}
