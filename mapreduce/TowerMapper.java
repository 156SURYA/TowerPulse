import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TowerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text tower = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // skip header
        if (value.toString().contains("call_id")) return;

        String[] fields = value.toString().split(",");
        tower.set(fields[6]);   // tower_id
        context.write(tower, one);
    }
}
