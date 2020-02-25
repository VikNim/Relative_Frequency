import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class nextPartitioner extends Partitioner<wordNext,IntWritable> {

    @Override
    public int getPartition(wordNext wordPair, IntWritable intWritable, int numPartitions) {
        return (wordPair.getWord().hashCode() & Integer.MAX_VALUE ) % numPartitions;
    }
}
