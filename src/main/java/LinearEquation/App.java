package LinearEquation;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws Exception {
        BufferedReader brA = new BufferedReader(new FileReader("./input/coefficient/coefficient.txt"));
        BufferedReader brX = new BufferedReader(new FileReader("./input/unknownNum/unknownNum.txt"));
        BufferedReader brB = new BufferedReader(new FileReader("./input/bvector/bvector.txt"));

        ArrayList<ArrayList<Float>> A = new ArrayList<ArrayList<Float>>();
        List<Float> X = new ArrayList<Float>();
        List<Float> B = new ArrayList<Float>();
        List<Float> newX = new ArrayList<Float>();

        String line;
        while ((line = brA.readLine()) != null) {
            String[] tmp = line.split("\\s+");
            String[] num = tmp[1].split(",");
            ArrayList<Float> inter = new ArrayList<Float>();
            for (int i = 0; i < num.length; i++)
                inter.add(Float.parseFloat(num[i]));
            A.add(inter);
        }

        while ((line = brB.readLine()) != null) {
            B.add(Float.parseFloat(line));
        }

        while ((line = brX.readLine()) != null) {
            X.add(Float.parseFloat(line));
        }

        brA.close();
        brB.close();
        brX.close();


        while (true) {
            for (int i = 0; i < A.size(); i++) {
                float sum = 0;
                for (int j = 0; j < A.size(); j++) {
                    if (i == j)
                        continue;
                    sum += A.get(i).get(j) * X.get(j);
                }
                newX.add((B.get(i) - sum) / A.get(i).get(i));
            }

            float tmpsum = 0;
            if (CheckError(X, newX, 0.5f))
                break;
            else {
                X.clear();
                X.addAll(newX);
                newX.clear();
            }
        }

        System.out.println(newX.toString());
    }

    public static boolean CheckError(List<Float> a, List<Float> b, float precision) {
        if (a.size() != b.size())
            throw new RuntimeException("the demonsion of a and b is not the same");
        double sum = 0;
        for (int i = 0; i < a.size(); i++) {
            sum += Math.pow(a.get(i) - b.get(i), 2);
        }

        if ( Math.sqrt(sum) <= precision)
            return true;
        else
            return false;
    }

}
