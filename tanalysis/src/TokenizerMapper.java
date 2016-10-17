import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    String range;
    int length, startRange;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String[] dump_array = value.toString().split(";"); //Fetch date from the dataset
	String[] date_array = dump_array[1].split(", "); //Split them with ,
	String date = date_array[0];

          data.set(date);
          context.write(data, one); //Each time we add a new data it will add 1 the value on that date.

    }
}
