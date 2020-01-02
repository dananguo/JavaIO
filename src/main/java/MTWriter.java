import java.io.*;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;


/**
 * @author 19028
 * @date 2019/11/23 10:21
 */

public class MTWriter {
    private final static int SIZE = 256;
    private final static int MAX_NUM = 2014 * 512;
    private int threadSize;
    private CyclicBarrier cyclicBarrier;
    private File file;
    private int method;

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.println("-----------------请输入线程数----------------");
        int threadSize = scanner.nextInt();
        System.out.println("---------------------------请输入写策略-------------------------\n" +
                "|     数字1:RandomAccessFile.write(byte[])（单次写入1024字节）  |\n" +
                "|     数字2:RandomAccessFile.write(byte[])（单次写入2048字节）  |\n" +
                "|     数字3:RandomAccessFile.write(byte[])（单次写入4096字节）  |\n" +
                "--------------------------------------------------------------");
        int method = scanner.nextInt();
        MTWriter mtWriter = new MTWriter(new File("data.txt"), threadSize, method);
        mtWriter.start();
    }

    private MTWriter(File out, int threadSize, int method) {
        this.file = out;
        this.threadSize = threadSize;
        this.cyclicBarrier = new CyclicBarrier(threadSize + 1);//加上主线程
        this.method = method;
    }

    private void start() throws Exception {
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

        SliceWriterThread(List<byte[]> slice, File file, CyclicBarrier cyclicBarrier, int startPos) {
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


