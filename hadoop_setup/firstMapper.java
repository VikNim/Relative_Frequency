import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class firstMapper extends Mapper<LongWritable, Text, wordNext, IntWritable> {
    private wordNext wordPair = new wordNext();
    private IntWritable ONE = new IntWritable(1);
    private IntWritable wordFrequency = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 1);
        String[] words = value.toString().split("\\s+");
        if (words.length > 1) {
            for (int i = 0; i < words.length; i++) {
                if(words[i].matches("^[A-Za-z]+$")) {
                    words[i] = words[i].replaceAll("\\W+","");

                    if(words[i].equals("")){
                        continue;
                    }

                    wordPair.setWord(words[i].toLowerCase());

                    int start = 0;
                    if(i > neighbors )
                    {
                       start = i - neighbors;
                    }

                    int end = i + neighbors;
                    if(i + neighbors >= words.length)
                    {
                        end = words.length - 1;
                    }

                    for (int j = start; j <= end; j++) {
                        if (j == i) continue;
                        if(words[j].matches("^[A-Za-z]+$")) {
                          words[j] = words[j].replaceAll("\\W","");
                          wordPair.setNeighbor(words[j].toLowerCase());
                          context.write(wordPair, ONE);
                        }
                    }

                    wordPair.setNeighbor("*");
                    wordFrequency.set(end - start);
                    context.write(wordPair, wordFrequency);
                }
            }
        }
    }
}
