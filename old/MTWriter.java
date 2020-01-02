import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CyclicBarrier;


/**
 * @author 19028
 * @date 2019/11/23 10:21
 */

public class MTWriter {
    private final static int SIZE = 4;
    private final static int MAX_NUM = 2;
    static boolean isCompleted = false;
    private static ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(SIZE);
    private Vector<Thread> threads;
    private int threadSize;
    private FileOutputStream fileOutputStream;
    private CyclicBarrier cyclicBarrier;
    private File file;
    private int method;

    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("-----------------请输入线程数----------------");
        int threadSize = scanner.nextInt();
        System.out.println("-----------------请输入写策略----------------\n" +
                "|             数字1:String转byte[]          |\n" +
                "|             数字2:int转byte[]             |\n" +
                "|             数字3:使用FileChannel         |\n" +
                "--------------------------------------------");
        int method = scanner.nextInt();
        MTWriter mtWriter = new MTWriter(new File("data.txt"), threadSize, method);
        mtWriter.start();
        if (isCompleted) {
            mtWriter.shutdown();
        }
    }

    private MTWriter(File out, int threadSize, int method) throws FileNotFoundException {
        this.file = out;
        this.threadSize = threadSize;
        this.threads = new Vector<>(threadSize);
        this.cyclicBarrier = new CyclicBarrier(threadSize + 1);//多一个生产者线程
        this.method = method;
        this.fileOutputStream = new FileOutputStream(out);
    }

    private void start() throws FileNotFoundException, InterruptedException {
        int sliceSize = SIZE / this.threadSize;
        Thread thread;
        thread = new Thread(new Producer(queue, cyclicBarrier, SIZE, MAX_NUM));
        final long startTime = System.currentTimeMillis();
        thread.start();

        for (int i = 0; i < threadSize; i++) {
            thread = new Thread(new SliceWriterThread(queue, sliceSize, i, file, cyclicBarrier, method));
            threads.add(thread);
            thread.start();
        }
        for (Thread thread1 : threads) {
            thread1.join();
        }

        System.out.println("总时间开销: " + (System.currentTimeMillis() - startTime));
    }

    private void shutdown() throws IOException {
        this.fileOutputStream.close();
    }

    static class SliceWriterThread extends Thread {
        private ArrayBlockingQueue<Integer> queue;
        private RandomAccessFile randomAccessFile;
        private CyclicBarrier cyclicBarrier;
        private int sliceSize;
        private int id;
        private long startPos;
        private int method;
        private FileChannel fileChannel;

        private SliceWriterThread(ArrayBlockingQueue<Integer> queue, int sliceSize, int id, File file, CyclicBarrier cyclicBarrier, int method) throws FileNotFoundException {
            this.queue = queue;
            this.sliceSize = sliceSize;
            this.id = id;
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.cyclicBarrier = cyclicBarrier;
            this.startPos = 0;
            this.method = method;
            this.fileChannel = randomAccessFile.getChannel();
        }

        private void setStartPos(int toProduce, int offset, int method) {
            int range = 0;
            switch (method) {
                case 1:
                case 3:
                    range = String.valueOf(toProduce).getBytes().length * sliceSize;
                    break;
                case 2:
                    range = 4;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + method);
            }

            int innerOffset = range * id;
            startPos = offset + innerOffset;
        }

        private void write(int toProduce, int method) throws IOException {
            switch (method) {
                case 1:
                    byte[] bytes = String.valueOf(toProduce).getBytes();
                    randomAccessFile.write(bytes);
                    break;
                case 2:
                    randomAccessFile.writeInt(toProduce);
                    break;
                case 3:
                    byte[] bytes1 = String.valueOf(toProduce).getBytes();
                    ByteBuffer buffer = ByteBuffer.allocate(bytes1.length);
                    buffer.clear();
                    buffer.put(bytes1);
                    buffer.flip();
                    fileChannel.write(buffer);
                    break;
                default:
                    break;
            }
        }

        @Override
        public void run() {
            try {
                int count = 0;
                int offset = 0;
                boolean isRanged = false;
                while (true) {
                    if (count == sliceSize) {
                        if (isCompleted) {
                            System.out.println("数据已全部生产完毕");
                            return;
                        }
                        isRanged = false;
                        count = 0;
                        cyclicBarrier.await();//等待其他线程执行完毕
                    }
                    int data = queue.take();
                    if (!isRanged) {
                        int length = 0;
                        switch (method) {
                            case 1:
                            case 3:
                                length = data == 1 ? 0 : String.valueOf(data - 1).getBytes().length;
                                break;
                            case 2:
                                length = data == 1 ? 0 : 4;//int 4字节
                                break;
                            default:
                                break;
                        }
                        offset += length * SIZE;
                        setStartPos(data, offset, method);
                        if(method == 3){
                            fileChannel.position(startPos);
                        } else if(method == 1){
                            randomAccessFile.seek(startPos);
                        }
                        isRanged = true;
                    }
                    count++;
                    write(data, method);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}


