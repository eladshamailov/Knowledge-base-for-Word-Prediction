import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class step6 {
	
	 /**
     * The Input:
     *      The input is the output of Step 5
     *         T  n-gram  /T  prob
     *         program is good   0.4
     *
     * The Output:
     *      Same data, in decending order organizing by the prob value.
     *        T  n-gram  /T  prob
     *        program is good  0.4
     *
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable k, Text value, Context context)  throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            Text key = new Text();
            key.set(splits[0]);
            Text newValue = new Text();
            newValue.set(splits[1]);
            context.write(key,newValue);
        }

    }
	
	private static class CompareClass extends WritableComparator {
	        protected CompareClass() {
	            super(Text.class, true);
	        }
	        @Override
	        public int compare(WritableComparable k1 , WritableComparable k2) {
	            String[] splits1 = k1.toString().split(" ");
	            String[] splits2 = k2.toString().split(" ");
	            if (splits1[0].equals(splits2[0])&& splits1[1].equals(splits2[1])) {
	                if(Double.parseDouble(splits1[3])>(Double.parseDouble(splits2[3]))){
	                        return 1;
	                    }
	                    else
	                        return -1;
	                }
	            return (splits1[0]+" "+splits1[1]).compareTo(splits2[0]+" "+splits2[1]);

	            }
	        }


	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
		job.setJarByClass(step6.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(step6.CompareClass.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
