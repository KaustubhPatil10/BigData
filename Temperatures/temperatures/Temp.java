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


public class Temp {
 
    public static class TempMapper extends Mapper <Object, /*Input key Type*/
    	    Text, /*Input value type*/
    	    Text, /*Output key Type*/
    	    FloatWritable  /*Output value type*/> {
           
    	// Map Function
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	 {
               
		String line[] = value.toString().split("\\s+");
	        float maxTemp = Float.parseFloat(line[5]);
	        String year = line[1].substring(0,4);
	        if(maxTemp > -60.0f && maxTemp < 60.0f){
	           context.write(new Text(year), new FloatWritable(maxTemp));
	        
		   }
    	      }
    	    }  	 
    	      /*    	// Reducer Class
    	public static class AvgSharesReducer extends MapReduceBase implements Reducer <IntWritable, IntWritable, IntWritable, FloatWritable>{
           
        	public void reduce(IntWritable key,
        			   Iterator <IntWritable> values,
        	   		   OutputCollector<IntWritable, FloatWritable> output,
        	   		   Reporter reporter ) throws IOException {
               
			int add = 0, total = 0;
			
			while(values.hasNext()) {
				add = add + values.next().get();
				total++;
			}
            		output.collect(key, new FloatWritable(add/(float)total));

        	}
        }
       */
    	public static void main(String args[]) throws Exception {
    
    		Configuration conf = new Configuration();
    		Job job = new Job(conf, "Temperature");
    		
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(FloatWritable.class);
    
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setInputFormatClass(TextInputFormat.class);
                
    		job.setMapperClass(TempMapper.class);
    		job.setNumReduceTasks(0);
    		
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Only 1 o\p path, inputs paths can be many.
    		job.waitForCompletion(true);
    
    	}
}
