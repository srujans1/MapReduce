/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package step3;
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
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 *
 * @author sru
 */
public class Step3 {

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
        job.setJarByClass(Step3.class);

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
            String[] node = value.toString().split(":");
            context.write(key, new Text(node[0]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        PriorityQueue<Pair> pQueue=new PriorityQueue<Pair>();
        TreeMap<Double, String> priorityQueue = new TreeMap<Double, String>();
    class Pair implements Comparable<Pair> {
    private String value;
    private Double key;

    public Pair(Double key, String value) {
        this.key   = key;
        this.value = value;
    }

    @Override
    public int compareTo(Pair o) {
        if (this.key > o.key)
            return 1;
        else if (this.key < o.key)
            return -1;
        return 0;
    }
}
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
         
            for (Text val : values)
            {
               Pair obj = new Pair(Double.parseDouble(val.toString()),key.toString());
            pQueue.add(obj);
             if (pQueue.size() > 100)
             {
                pQueue.poll();
             }
            
             
            }
        }
  
        
@Override
        protected void cleanup(Context context)
                throws IOException,
                InterruptedException {
        List<Pair> pArr= new ArrayList<Pair>();
         int i=0;
            while (!pQueue.isEmpty()) {
                Pair entry = pQueue.poll();
                pArr.add(entry);
                }
            Collections.reverse(pArr);
            for(Pair p:pArr)
            {
            context.write(new Text(p.value.toString()), new Text(p.key.toString()));
            }
            
        }
    }






}

