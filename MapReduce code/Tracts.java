import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        
public class Tracts {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tractLine = value.toString().split("\t");		
		
        
        Pattern pattern = Pattern.compile("14000US360(\\d{2})(.*)");
		Matcher match = pattern.matcher(tractLine[0]);
		if (match.find()) {
			 String boroughCode = match.group(1).toString();
			 String newKey = match.group(2).toString();
			 System.out.println("match " + newKey + " with: " + tractLine[0] + "borough code: " + boroughCode);
			 String newValue = tractLine[0] + "\t" + boroughCode + "\t" + tractLine[1] + "\t" + tractLine[2];
			 System.out.println("New line: " + newValue);
			 context.write(new Text(newKey), new Text(newValue));
		 }
    }
 } 
        
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "tracts");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
 //   job.setReducerClass(Reduce.class);
        
 //   job.setInputFormatClass(TextInputFormat.class);
  //  job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Temp.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}