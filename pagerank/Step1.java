/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 *
 * @author sru
 */
public class Step1 {

    /**
     * @param args the command line arguments
     */
        public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration());
        
       
        String input="";
        String output="";
   
       
        for(int i=0;i<args.length;i++)
        {
           if(args[i].toString().equals("-input"))
           {
           input=args[++i].toString();
           continue;
           }
              
           if(args[i].toString().equals("-output"))
           {        
            output=args[++i].toString();
            continue;
           }
       
           
        }
        
        
        
        job.setJarByClass(Step1.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

   
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String pageRank = "1.0:"; // initialization of PR
                        boolean first = true;
                        for (Text val : values)
                        {
                                if(!first) {
                                        pageRank += ",";
                                }
                                pageRank += val.toString();
                                first = false;
                        }
                        context.write(key, new Text(pageRank));
          
        }
    }
}
