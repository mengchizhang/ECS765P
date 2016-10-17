


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
    	 String dump = value.toString();
    	 if(StringUtils.ordinalIndexOf(dump,";",4)>-1){ //In this step we assign the string we get from data set into tweet.
    	        int startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
    	        String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
    	      
    	 length = tweet.length();
    	 }

    	 if (length < 141){ //We will only pick the tweet that length is less than 141 because the maximum length of tweet is 140.
         int category = length/5;

         if(length%5==0){ //Aggregate bars in group of 5 and make sure the 5th,10th..etc only be counted into 1 category
                 startRange = ((category -1 ) * 5) + 1;
         }

         else{    
                 startRange = (category * 5) + 1;
         }

         int endRange = startRange + 4;

         range = startRange + "-" + endRange;
         data.set(range);
         context.write(data, one);
        }
    }
}
