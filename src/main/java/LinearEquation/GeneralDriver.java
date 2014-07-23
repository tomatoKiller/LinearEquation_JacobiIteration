package LinearEquation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
//import org.dom4j.Document;
////import org.dom4j.DocumentException;
//import org.dom4j.Element;
//import org.dom4j.io.SAXReader;

import java.net.URI;
import java.util.concurrent.TimeUnit;

enum PrecisionRequire {
    ITEM
}

public class GeneralDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
//        String ConfXml = GeneralDriver.class.getClassLoader().getResource("linear_equation.xml").getPath();
//        int res = ToolRunner.run(new Configuration(), new GeneralDriver(), AnalyzeXml(ConfXml));
        int res = ToolRunner.run(new Configuration(), new GeneralDriver(), AnalyzeXml("./linear_equation.xml"));

        System.exit(res);
    }

    public static final Logger log = LogManager.getLogger(GeneralDriver.class);

    /*
    @return String[0]: inputPath    String[1]: outputPath
     */
    private static String[] AnalyzeXml(String xmlPath) {
          String[] ret = new String[2];
        ret[0] = "hdfs://192.168.1.191:8020/algm_home/linear_equation/input/";
        ret[1] = "hdfs://192.168.1.191:8020/algm_home/linear_equation/output/";
//        SAXReader reader = new SAXReader();
//        Document document = null;
//        try {
//            document = reader.read(xmlPath);
//        } catch (Exception e) {
//            log.error(e.getMessage());
//            log.error("GeneralDriver job failed");
//            e.printStackTrace();
//        }

//        Element root = document.getRootElement();
//        Element input = root.element("input");
//        ret[0] = input.elementText("filename");
//        Element output = root.element("output");
//        ret[1] = output.elementText("filename");
        return ret;
    }

    @Override
    public int run(String[] args) throws Exception {



        int round = 0;
        long start = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);


        String input = args[0];
        String output = args[1];

        while (true) {

            Configuration conf = getConf();
            Job job = new Job(conf, "LinearEquation-"+round);

            job.setJarByClass(GeneralDriver.class);

//            job.setInputFormatClass(TextInputFormat.class);
            job.setInputFormatClass(NLineInputFormat.class);
            NLineInputFormat.setNumLinesPerSplit(job, 200);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(GeneralMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(GeneralReduce.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(input + "coefficient/"));

            job.setNumReduceTasks(1);

            job.getConfiguration().setFloat("error", 0.05f);


            Path outputDir = new Path(output + "round" + round);

            if (FileSystem.get(job.getConfiguration()).exists(outputDir))
                FileSystem.get(job.getConfiguration()).delete(outputDir, true);

            FileOutputFormat.setOutputPath(job, outputDir);

            String inputX;
            if (round == 0){
                inputX = input + "unknownNum/unknownNum.txt";
            } else {
                inputX = output + "round" + (round - 1) + "/part-r-00000";
            }

            DistributedCache.addCacheFile(new URI(inputX), job.getConfiguration());
            DistributedCache.addCacheFile(new URI(input + "bvector/bvector.txt"), job.getConfiguration());

            if (!job.waitForCompletion(true))
                throw new RuntimeException("job failed");

            //满足精度要求
            if (job.getCounters().findCounter(PrecisionRequire.ITEM).getValue() > 0)
                break;

            round++;
            long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            if (now - start > 5400)
                System.exit(5);

        }

        long end = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        System.out.println("total time = " + (end - start) + "s" );
        return  0;
    }
}
