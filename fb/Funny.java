//
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;


public class Funny {
 
    public static class FunnyMapper extends Mapper <Object, /*Input key Type*/
    	    Text, /*Input value type*/
    	    Text, /*Output key Type*/
    	    IntWritable  /*Output value type*/> {
           
    	// Map Function
    	boolean flag = false;  //flag is set flase to skip the first row, check 43
    	int max = 0, rownumber = 0;
    	String type = null;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	 {
               
		String line[] = value.toString().split(",", 12);
		if(flag) {
		  rownumber++;
		  if(Integer.parseInt(line[9]) > max) {
		     max = Integer.parseInt(line[9]);
		     type = rownumber + "\t" + line[1] + "\t" + line[2];
		   }
        	  }     	
        	flag = true;
    	      }
    	// cleanup called once at the end of Mapper      
    	public void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text(type), new IntWritable(max));  
    	}
    }	
    	   
    	public static void main(String args[]) throws Exception {
    
    		Configuration conf = new Configuration();
    		Job job = new Job(conf, "Funniest post");
    		
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(IntWritable.class);
    
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setInputFormatClass(TextInputFormat.class);
                
    		job.setMapperClass(FunnyMapper.class);
    		job.setNumReduceTasks(0);
    		
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Only 1 o\p path, inputs paths can be many.
    		job.waitForCompletion(true);
    
    	}
}
