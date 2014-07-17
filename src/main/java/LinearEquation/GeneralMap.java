package LinearEquation;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wu on 14-7-15.
 */
public class GeneralMap extends Mapper<Object, Text, Text, Text> {

    private URI[] files;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        files = DistributedCache.getCacheFiles(context.getConfiguration());

    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        List<Float> b = new ArrayList<Float>(); //b向量
        List<Float> x = new ArrayList<Float>(); //未知数x向量
        List<Float> a = new ArrayList<Float>(); //系数矩阵某行向量

        if (files!= null) {
            for (URI p : files) {
                FileSystem fs = FileSystem.get(p, context.getConfiguration());
                BufferedReader reader = null;
                try {
                    FSDataInputStream in = fs.open(new Path(p.getPath()));
                    reader = new BufferedReader(new InputStreamReader(in));
                    String str;
                    while ((str = reader.readLine()) != null) {

                        if (p.getPath().indexOf("bvector") != -1) {
                            //b向量
                            b.add(Float.parseFloat(str));
                        }
                        if (p.getPath().indexOf("unknownNum") != -1 || p.getPath().contains("output")) {
                            // x向量
                            x.add(Float.parseFloat(str));
                        }
                    }
                } finally {
                    if (reader != null)
                        reader.close();
                }



            }


            String[] tmp = value.toString().split("\\s+");
            int line = Integer.parseInt(tmp[0]);
            String[] item = tmp[1].split(",");
            for (int i = 0; i < item.length; i++) {
                //在矩阵A的基础上加上单位阵E
                float e = 0f;
                if (i+1 == line)
                    e++;
                a.add(Float.parseFloat(item[i]) + e);
            }

            float sum = 0.0f;
            for (int i = 0; i < a.size(); i++)
                sum += a.get(i) * x.get(i);

            sum -= b.get(line - 1);

            //将key全部设为'x'，统一由一个Reducer处理
            String strs = String.valueOf(sum);
            context.write(new Text("x"), new Text(line-1 + ":" + strs));

        }

    }
}
