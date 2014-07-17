package LinearEquation;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Created by wu on 14-7-15.
 */
public class GeneralReduce extends Reducer<Text, Text, NullWritable, Text> {

    private URI[] files;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        files = DistributedCache.getCacheFiles(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //利用TreeMap 对values中的x排序
        Map<Integer, Float> newX = new TreeMap<Integer, Float>();
        Queue<Float> oldX = new LinkedList<Float>();

        for (Text val : values) {
            String[] tmp = val.toString().split(":");
            newX.put(Integer.parseInt(tmp[0]), Float.parseFloat(tmp[1]));
        }

        if (files!= null) {

            URI p = files[0];

            FileSystem fs = FileSystem.get(p, context.getConfiguration());
            BufferedReader reader = null;
            try {
                FSDataInputStream in = fs.open(new Path(p.getPath()));
                reader = new BufferedReader(new InputStreamReader(in));


                if (p.getPath().indexOf("unknownNum") != -1 || p.getPath().contains("output")) {
                    // x向量
                    String str;
                    while ((str= reader.readLine()) != null)
                        oldX.offer(Float.parseFloat(str));
                } else {
                    throw new RuntimeException("wrong cached file path in GeneralReduce:59");
                }
            } finally {
                if (reader != null)
                    reader.close();
            }

        } else {
            throw new RuntimeException("cache files are null");
        }

        float inaccuracy = 0.0f;
        for (Map.Entry<Integer, Float> en : newX.entrySet()) {
            context.write(null,new Text(String.valueOf(en.getValue())));
            inaccuracy += Math.pow(en.getValue() - oldX.poll(), 2);
        }

        inaccuracy = (float)Math.sqrt(inaccuracy);

        float error = context.getConfiguration().getFloat("error", 0.5f);   //默认精度为0.5

        //满足精度要求
        if (inaccuracy < error)
            context.getCounter(PrecisionRequire.ITEM).increment(1L);


    }
}
