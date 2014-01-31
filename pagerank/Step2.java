/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package step2;
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
import java.util.ArrayList;

/**
 *
 * @author sru
 */
public class Step2 {

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
        
        
        job.setJarByClass(Step2.class);

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

    public static class Map extends Mapper<Text, Text, Text, Text>
    {
       @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException 
       {
           String[] parts = value.toString().split(":");
                        
                        if(parts.length > 1) {
                                String PRStr = parts[0];
                                String nodesStr = parts[1];
                                String[] nodes = nodesStr.split(",");
                                
                                int count = nodes.length;
                                for(int i = 0; i < nodes.length; i++) {
                                        String tmp = PRStr;
                                        tmp += ":";
                                        tmp += Integer.toString(count);
                                        context.write(new Text(nodes[i]), new Text(tmp));
                                }
                                context.write(key, new Text(nodesStr));
                        } 
       }
        }
   

   

    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {
       @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
       {
             ArrayList<String> values2 = new ArrayList<String>();
                         for (Text val : values)
                         {
                                String value = val.toString();
                                values2.add(value);
                         }
                        
                        String nodesStr = "";
                        
                  
                        float damping = 0.85f;
                        
                        float newPR = 0.0f;
                        float sum = 0.0f;
                        for(int i = 0; i < values2.size(); i++) {
                                String value = values2.get(i);
                                String[] parts = value.split(":");
                                if(parts.length > 1) {
                                        float PR = Float.parseFloat(parts[0]);
                                        int links = Integer.parseInt(parts[1]);
                                        sum += (PR/links);
                                } else if(parts.length == 1) {
                                        
                                        nodesStr = value;
                                }
                        }
                        newPR = (sum*damping + (1-damping)); // updating PageRank
                        String tmp = Float.toString(newPR);
                        tmp += ":";
                        tmp += nodesStr;
              
                        context.write(key, new Text(tmp));
            
       }
    }
}
