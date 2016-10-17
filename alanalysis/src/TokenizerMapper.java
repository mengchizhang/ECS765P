import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private IntWritable t = new IntWritable();
    private Text data = new Text();
    String range;
    int length, startRange;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	 String dump = value.toString();
    	 if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
    	        int startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
    	        String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
    	      
    	      
    	 length = tweet.length();
    	 }
       if (length < 141){
	   t.set((int) length); //Parsing the length value to the t
          data.set("Mengchi");
          context.write(data, t);
        }
    }
}
