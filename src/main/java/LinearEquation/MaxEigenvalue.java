package LinearEquation;

import java.io.*;
import java.util.*;

/**
 * Created by wu on 2014/7/17.
 */
public class MaxEigenvalue {
    List<Double> X = new ArrayList<Double>();
    ArrayList<ArrayList<Double>> A = new ArrayList<ArrayList<Double>>();
    private double precision;
    private int maxIterNum = 1000;
    private String  coefficient;
    private String  initialVector;
    private double ev;

    public MaxEigenvalue(String coefficient, String initialVector, double precision, int maxIterNum) throws IOException {
        this.precision = precision;
        this.maxIterNum = maxIterNum;
        this.coefficient = coefficient;
        this.initialVector = initialVector;
        ReadData();
        ev = getEV();
    }

    public void setPrecision(double precision) {
        this.precision = precision;
    }

    public void setMaxIterNum(int i) {
        this.maxIterNum = i;
    }

    public void ReadData() throws IOException {

        File tmpfile = new File("./input");
        if (!tmpfile.exists())
            tmpfile.mkdir();

        tmpfile = new File(coefficient);
        if (!tmpfile.exists())
            tmpfile.mkdir();

        tmpfile = new File(initialVector);
        if (!tmpfile.exists())
            tmpfile.mkdir();

        tmpfile = new File("./input/unknownNum");
        if (!tmpfile.exists())
            tmpfile.mkdir();

        int N = 100;

        //随机生成向量b
        BufferedWriter bx = null;
        bx = new BufferedWriter(new FileWriter("./input/bvector/bvector.txt"));

        for (int i = 0; i < N; i++) {
            bx.write(String.valueOf(i/2.0));
            bx.write("\n");
        }
        bx.close();

        //随即生成向量x
        bx = new BufferedWriter(new FileWriter("./input/unknownNum/unknownNum.txt"));
        for (int i = 0; i < N; i++) {
            X.add((double)i);
            bx.write(String.valueOf(i));
            bx.write("\n");
        }
        bx.close();

        //随即生成系数矩阵A
        Random rand = new Random(47);

        bx = new BufferedWriter(new FileWriter("./input/coefficient/coefficient.txt"));
        for (int i = 0; i < N; i++) {
            A.add(new ArrayList<Double>());
            for (int j = 0; j < N; j++) {
                if (i <= j) {
                    double tmp = rand.nextDouble();
                    A.get(i).add(tmp);
                    bx.write(String.valueOf(tmp));
                } else {
                    A.get(i).add(A.get(j).get(i));
                    bx.write(String.valueOf(A.get(j).get(i)));
                }
                bx.write(" ");
            }
            bx.write("\n");

        }
        bx.close();

    }

    private double getMax(List<Double> l) {
        double max = 0;
        for (double item : l) {
            if (max < item)
                max = item;
        }
        return max;
    }

    public double getEigenValue() {
        return ev;
    }

    private double VectorMultiply(List<Double> a, List<Double> b) {
        if (a.size() != b.size())
            throw new RuntimeException("the demonsion of a and b is not the same : " + a.size());
        double sum = 0;
        for (int i = 0; i < a.size(); i++) {
            sum += a.get(i) * b.get(i);
        }
        return sum;
    }

    private boolean CheckError(List<Double> a, List<Double> b) {
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

    private double getEV() {
        List<Double> Y = new ArrayList<Double>();
        List<Double> newX = new ArrayList<Double>();

        int num = 0;
        while (num++ < maxIterNum ) {
            double max = getMax(X);

            //进行归一化处理


            for (int i = 0; i < X.size(); i++) {
                Y.add(X.get(i) / max);
            }

            for (int i = 0; i < A.size(); i++) {
                newX.add(VectorMultiply(A.get(i), Y));
            }

            if (CheckError(X, newX)) {
                double m = getMax(X);
//                for (double i : X)
//                    System.out.print(i/m + " ");
                return getMax(X);
            }
            else {
                X.clear();
                X.addAll(newX);
                newX.clear();
                Y.clear();
            }
        }
        System.out.println("error");
        return 0;
    }

    public void ProduceMatrix() {

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(coefficient));
            for (int i = 0; i < A.size(); i++) {
                bw.write((i+1) + "\t");
                for (int j = 0; j < A.size(); j++) {
                    if (i == j)
                        bw.write(String.valueOf(A.get(i).get(j) / (ev + 1) - 1));
                    else
                        bw.write(String.valueOf(A.get(i).get(j) / (ev + 1)));
                    bw.write(",");
                }
                bw.write("\n");
            }

            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {


        MaxEigenvalue me = new MaxEigenvalue("./input/coefficient/coefficient.txt", "./input/bvector/bvector.txt", 0.1, 2000000);

        me.ProduceMatrix();

        System.out.println(me.getEigenValue());



    }
}
