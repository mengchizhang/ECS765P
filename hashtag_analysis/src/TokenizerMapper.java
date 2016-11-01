import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Locale;  //Import all the countrycode which includes in the Locate
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public static String countryCode[];
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	String tweets[] = value.toString().split(";"); // Split dataset with ;
    countryCode = Locale.getISOCountries();

	String hashtag =tweets[2].toString(); // Find the hashtag in the dataset
	hashtag = hashtag.toUpperCase();
	for(int i=0;i<countryCode.length;i++){
		
		if (hashtag.indexOf("TEAM" + countryCode[i].toUpperCase()) != -1 || hashtag.indexOf("GO" + countryCode[i].toUpperCase()) != -1) { // Only execute when the tweets contains TEAM or GO
				data.set(countryCode[i]);
				context.write(data, one);  			
		}
	}
    }
}
