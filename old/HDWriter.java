//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//
//import java.io.*;
//import java.net.URISyntaxException;
//import java.util.Scanner;
//import java.util.Vector;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.CyclicBarrier;
//
//
///**
// * @author 19028
// * @date 2019/11/23 10:21
// */
//
//public class HDWriter {
//    private final static int SIZE = 4;
//    private final static int MAX_NUM = 2;
//    static boolean isCompleted = false;
//    private static ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(SIZE);
//    private Vector<Thread> threads;
//    private int threadSize;
//    private FSDataOutputStream fsDataOutputStream;
//    private CyclicBarrier cyclicBarrier;
//    private int method;
//    private FileSystem fileSystem;
//
//    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
//        Scanner scanner = new Scanner(System.in);
//        System.out.println("-----------------请输入线程数----------------");
//        int threadSize = scanner.nextInt();
//        System.out.println("-----------------请输入写策略----------------\n" +
//                "|             数字1:String转byte[]          |\n" +
//                "|             数字2:int转byte[]             |\n" +
//                "|             数字3:使用FileChannel         |\n" +
//                "--------------------------------------------");
//        int method = scanner.nextInt();
//
//        HDWriter mtWriter = new HDWriter(threadSize, method);
//        mtWriter.start();
//        if (isCompleted) {
//            mtWriter.shutdown();
//        }
//    }
//
//    private HDWriter(int threadSize, int method) throws IOException, URISyntaxException, InterruptedException {
//        this.threadSize = threadSize;
//        this.threads = new Vector<>(threadSize);
//        this.cyclicBarrier = new CyclicBarrier(threadSize + 1);//多一个生产者线程
//        this.method = method;
//        this.fsDataOutputStream = this.getFileSystem().create(new Path("/data.txt"));
//    }
//
//    private void start() throws InterruptedException {
//        int sliceSize = SIZE / this.threadSize;
//        Thread thread;
//        thread = new Thread(new Producer(queue, cyclicBarrier, SIZE, MAX_NUM));
//        final long startTime = System.currentTimeMillis();
//        thread.start();
//
//        for (int i = 0; i < threadSize; i++) {
//            thread = new Thread(new SliceWriterThread(queue, sliceSize, fsDataOutputStream, cyclicBarrier, method));
//            threads.add(thread);
//            thread.start();
//        }
//        for (Thread thread1 : threads) {
//            thread1.join();
//        }
//
//        System.out.println("总时间开销: " + (System.currentTimeMillis() - startTime));
//    }
//
//    private void shutdown() throws IOException {
//        if (fileSystem != null) {
//            fsDataOutputStream.close();
//            fsDataOutputStream = null;
//        }
//    }
//
//    private FileSystem getFileSystem() throws IOException, URISyntaxException, InterruptedException {
//        if (fileSystem == null) {
//            Configuration configuration = new Configuration();
//            configuration.set("dfs.client.use.datanode.hostname", "true");//坑1
//            configuration.set("fs.defaultFS", "hdfs://aliyun:9000");//坑2
//            System.setProperty("HADOOP_USER_NAME", "root");
//            fileSystem = FileSystem.get(configuration);
//        }
//        return fileSystem;
//    }
//
//    static class SliceWriterThread extends Thread {
//        private ArrayBlockingQueue<Integer> queue;
//        private CyclicBarrier cyclicBarrier;
//        private int sliceSize;
//        private int method;
//        private FSDataOutputStream fsDataOutputStream;
//
//        private SliceWriterThread(ArrayBlockingQueue<Integer> queue, int sliceSize,
//                                  FSDataOutputStream fsDataOutputStream, CyclicBarrier cyclicBarrier,
//                                  int method) {
//            this.queue = queue;
//            this.sliceSize = sliceSize;
//            this.cyclicBarrier = cyclicBarrier;
//            this.method = method;
//            this.fsDataOutputStream = fsDataOutputStream;
//        }
//
////        private void setStartPos(int toProduce, int offset, int method) {
////            int range = 0;
////            switch (method) {
////                case 1:
////                    range = String.valueOf(toProduce).getBytes().length * sliceSize;
////                    break;
////                case 2:
////                    range = 4;
////                    break;
////                case 3:
////                    break;
////                default:
////                    throw new IllegalStateException("Unexpected value: " + method);
////            }
////
////            int innerOffset = range * id;
////            startPos = offset + innerOffset;
////        }
//
//        private void write(int toProduce, int method) throws IOException {
//            System.out.println(toProduce);
//            switch (method) {
//                case 1:
//                    byte[] bytes = String.valueOf(toProduce).getBytes();
//                    fsDataOutputStream.write(bytes);
//                    break;
//                case 2:
//                    fsDataOutputStream.writeInt(toProduce);
//                    break;
//                case 3:
//                    break;//TODO:
//                default:
//                    break;
//            }
//        }
//
//        @Override
//        public void run() {
//            try {
//                int count = 0;
////                int offset = 0;
////                boolean isRanged = false;
//                while (true) {
//                    if (count == sliceSize) {
//                        if (isCompleted) {
//                            System.out.println("数据已全部生产完毕");
//                            return;
//                        }
////                        isRanged = false;
//                        count = 0;
//                        cyclicBarrier.await();//等待其他线程执行完毕
//                    }
//                    int data = queue.take();
////                    if (!isRanged) {
////                        int length = 0;
////                        switch (method) {
////                            case 1:
////                                length = data == 1 ? 0 : String.valueOf(data - 1).getBytes().length;
////                                break;
////                            case 2:
////                                length = data == 1 ? 0 : 4;//int 4字节
////                                break;
////                            case 3://TODO:
////                            default:
////                                break;
////                        }
////                        offset += length * SIZE;
////                        setStartPos(data, offset, method);
////                        fsDataOutputStream.seek(startPos);
////                        isRanged = true;
////                    }
//                    count++;
//                    write(data, method);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}
//
//
