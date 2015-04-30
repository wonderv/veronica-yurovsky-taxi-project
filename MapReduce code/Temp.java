import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        
public class Temp {
        
 public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] weatherLine = value.toString().split("\\s+");		
		
		if (weatherLine[22].indexOf('*') == -1){
			long timeStamp = Long.parseLong(weatherLine[2]);
			String strHourly = Long.toString(timeStamp);
			char[] digits = strHourly.toCharArray();
			String timestamp = "2013-" + digits[4] + digits[5] + "-" + digits[6] + digits[7] + " " + digits[8] + digits[9] + ":" + "00:00";
			float temp = Float.parseFloat(weatherLine[22]);
			System.out.println("long: " + timeStamp + ", string: " + strHourly + ", timestamp: " + timestamp);
			context.write(new Text(timestamp), new FloatWritable(temp));
			//System.out.println("Just wrote day: " + timestamp + " and temp: " + temp);
		}
    }
 } 
        
 public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
    	float sum = 0;
    	int num = 0;
    	
		for(FloatWritable value : values) {
			sum += Float.parseFloat(value.toString());
			num++;
		}
    	
    	float avgTemp = sum/num;
    	
		//System.out.println("Printing after reducing " + key + " " + avgTemp);
		context.write(key, new FloatWritable(avgTemp));
	    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "temp");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
 //   job.setInputFormatClass(TextInputFormat.class);
  //  job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Temp.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}