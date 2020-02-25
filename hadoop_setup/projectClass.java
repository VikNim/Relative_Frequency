import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class projectClass {

    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {

        //1st stage Mapreduce job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(projectClass.class);
        job.setJobName("projectClass");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("output1"));

        job.setMapperClass(firstMapper.class);
        job.setReducerClass(firstReducer.class);
        job.setCombinerClass(nextReducer.class);
        job.setPartitionerClass(nextPartitioner.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(wordNext.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);

        //2nd stage Mapreduce job       
        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(projectClass.class);
        job2.setJobName("projectClass");

        job2.setSortComparatorClass(resultClass.class);
        FileInputFormat.addInputPath(job2, new Path("output1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setMapperClass(lastMapper.class);
        job2.setReducerClass(lastReducer.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(wordNext.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
