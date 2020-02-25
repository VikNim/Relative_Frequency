import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class nextReducer extends Reducer<wordNext,IntWritable,wordNext,IntWritable> {
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(wordNext key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}
