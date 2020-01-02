import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


/**
 * @author 19028
 * @date 2019/11/23 10:21
 */

public class HDWriter {
    private final static int SIZE = 256;
    private final static int MAX_NUM = 2014 * 512;
    private int threadSize;
    private CyclicBarrier cyclicBarrier;
    private int method;
    private File file;

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
//        System.out.println("-----------------请输入线程数----------------");
//        int threadSize = scanner.nextInt();
        System.out.println("---------------------------请输入写策略-------------------------\n" +
                "|     数字1:RandomAccessFile.write(byte[])（单次写入1024字节）  |\n" +
                "|     数字2:RandomAccessFile.write(byte[])（单次写入2048字节）  |\n" +
                "|     数字3:RandomAccessFile.write(byte[])（单次写入4096字节）  |\n" +
                "--------------------------------------------------------------");
        int method = scanner.nextInt();
        int[] threadSizes = {32, 16, 8, 4, 2, 1};
        for (int threadSize : threadSizes) {
            HDWriter hdWriter = new HDWriter(new File("data.txt"), threadSize, method);
            hdWriter.start();
        }
    }

    private HDWriter(File out, int threadSize, int method) {
        this.file = out;
        this.threadSize = threadSize;
        this.cyclicBarrier = new CyclicBarrier(threadSize + 1);//加上主线程
        this.method = method;
    }

    public void upload() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("dfs.client.use.datanode.hostname", "true");//坑1
            configuration.set("fs.defaultFS", "hdfs://aliyun:9000");//坑2
            System.setProperty("HADOOP_USER_NAME", "root");
            FileSystem fileSystem = FileSystem.get(configuration);
            InputStream inputStream = new FileInputStream("data.txt");
            OutputStream outputStream = fileSystem.create(new Path("/data.txt"));
            IOUtils.copyBytes(inputStream, outputStream, configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void start() throws BrokenBarrierException, InterruptedException {
        int sliceSize = setSliceSize();
        Producer producer = new Producer(SIZE, MAX_NUM, method);
        final long startTime = System.currentTimeMillis();

        //得到装有每个数生成256次的总列表
        List<byte[]> list = producer.produce();
        int startPos = 0;
        Thread thread;
        for (int i = 0; i < threadSize; i++) {
            List<byte[]> slice = list.subList(startPos, sliceSize * (i + 1));
            thread = new Thread(new SliceWriterThread(slice, file, cyclicBarrier, MAX_NUM / threadSize));
            thread.start();
            startPos += sliceSize;
        }
        cyclicBarrier.await();
        upload();
        System.out.println("总时间开销: " + (System.currentTimeMillis() - startTime));
    }

    private int setSliceSize() {
        int sliceSize = 0;
        switch (method) {
            case 1: {
                sliceSize = MAX_NUM / threadSize;
                break;
            }
            case 2: {
                sliceSize = MAX_NUM / threadSize / 2;
                break;
            }
            case 3: {
                sliceSize = MAX_NUM / threadSize / 4;
                break;
            }
        }
        return sliceSize;
    }

    static class SliceWriterThread extends Thread {
        private List<byte[]> slice;
        private CyclicBarrier cyclicBarrier;
        private long startPos;
        private File file;

        private SliceWriterThread(List<byte[]> slice, File file, CyclicBarrier cyclicBarrier, int startPos) {
            this.slice = slice;
            this.cyclicBarrier = cyclicBarrier;
            this.startPos = startPos * 4;
            this.file = file;
        }

        @Override
        public void run() {
            RandomAccessFile randomAccessFile;
            try {
                randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.seek(startPos);
                for (byte[] bytes : slice) {
                    randomAccessFile.write(bytes);
                }
                cyclicBarrier.await();
                randomAccessFile.close();
            } catch (IOException | InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }
}

