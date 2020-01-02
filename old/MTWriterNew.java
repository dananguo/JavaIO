import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.*;


/**
 * @author 19028
 * @date 2019/11/23 10:21
 */

public class MTWriterNew {
    private final static int SIZE = 256;
    private final static int MAX_NUM = 2014 * 512;
    static boolean isCompleted = false;
    private static LinkedBlockingQueue<Queue<Integer>> queue = new LinkedBlockingQueue<>();
    private Vector<Thread> threads;
    private int threadSize;
    private FileOutputStream fileOutputStream;
    private CyclicBarrier cyclicBarrier;
    private File file;
    private int method;
    private ExecutorService executorService;

    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("-----------------请输入线程数----------------");
        int threadSize = scanner.nextInt();
        System.out.println("-----------------请输入写策略----------------\n" +
                "|             数字1:String转byte[]（分批写入）          |\n" +
                "|             数字2:int转byte[]（逐个写入）             |\n" +
                "|             数字3:使用FileChannel（逐个写入）         |\n" +
                "--------------------------------------------");
        int method = scanner.nextInt();
        MTWriterNew mtWriter = new MTWriterNew(new File("data.txt"), threadSize, method);
        mtWriter.start();
        if (isCompleted) {
            mtWriter.shutdown();
        }
    }

    private MTWriterNew(File out, int threadSize, int method) throws FileNotFoundException {
        this.file = out;
        this.threadSize = threadSize;
        this.threads = new Vector<>(threadSize);
        this.cyclicBarrier = new CyclicBarrier(threadSize + 1);//多一个生产者线程
        this.method = method;
        this.fileOutputStream = new FileOutputStream(out);
        this.executorService = Executors.newFixedThreadPool(threadSize + 1);
    }

    private void start() throws FileNotFoundException, InterruptedException {
        int sliceSize = SIZE / threadSize;
        Thread thread;
        executorService.execute(new Producer(queue, cyclicBarrier, SIZE, MAX_NUM, method, threadSize));
//        thread = new Thread(new Producer(queue, cyclicBarrier, SIZE, MAX_NUM, method, threadSize));
        final long startTime = System.currentTimeMillis();
//        thread.start();

        for (int i = 0; i < threadSize; i++) {
//            thread = new Thread(new SliceWriterThread(queue, sliceSize, i, file, cyclicBarrier, method));
//            threads.add(thread);
//            thread.start();
            executorService.execute(new SliceWriterThread(queue, sliceSize, i, file, cyclicBarrier, method));
        }
        executorService.shutdown();

        System.out.println("总时间开销: " + (System.currentTimeMillis() - startTime));
    }

    private void shutdown() throws IOException {
        this.fileOutputStream.close();
    }

    static class SliceWriterThread extends Thread {
        private LinkedBlockingQueue<Queue<Integer>> queue;
        private RandomAccessFile randomAccessFile;
        private CyclicBarrier cyclicBarrier;
        private int sliceSize;
        private int id;
        private long startPos;
        private int method;
        private FileChannel fileChannel;

        private SliceWriterThread(LinkedBlockingQueue<Queue<Integer>> queue, int sliceSize, int id, File file, CyclicBarrier cyclicBarrier, int method) throws FileNotFoundException {
            this.queue = queue;
            this.sliceSize = sliceSize;
            this.id = id;
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.cyclicBarrier = cyclicBarrier;
            this.startPos = 0;
            this.method = method;
            this.fileChannel = randomAccessFile.getChannel();
        }

        private void setStartPos(int toProduce, int offset) {
            int range = String.valueOf(toProduce).getBytes().length * sliceSize;
            int innerOffset = range * id;
            startPos = offset + innerOffset;
        }

        private byte[] intToByteArray(int value) {
            byte[] src = new byte[4];
            src[0] = (byte) ((value >> 24) & 0xFF);
            src[1] = (byte) ((value >> 16) & 0xFF);
            src[2] = (byte) ((value >> 8) & 0xFF);
            src[3] = (byte) (value & 0xFF);
            return src;
        }

        private void write(Queue<Integer> data, int method) throws IOException {
            if (data == null) {
                return;
            }
            int toProduce;
            int offset;
            byte[] bytes;
            toProduce = data.poll();
            switch (method) {
                case 1:
                    randomAccessFile.seek(startPos);
                    StringBuilder str = new StringBuilder();
                    str.append(toProduce);
                    while (data.size() != 0) {
                        str.append(toProduce);
                        toProduce = data.poll();
                    }
                    bytes = str.toString().getBytes();
                    randomAccessFile.write(bytes);
                    break;
                case 2:
                    offset = (toProduce - 1) * SIZE * 4;
                    randomAccessFile.seek(offset);
                    while (data.size() != 0) {
                        System.out.println(toProduce);
                        randomAccessFile.writeInt(toProduce);
                        toProduce = data.poll();
                    }
                    break;
                case 3:
                    offset = (toProduce - 1) * SIZE * 4;
                    fileChannel.position(offset);
                    while (data.size() != 0) {
                        bytes = intToByteArray(toProduce);
                        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
                        buffer.clear();
                        buffer.put(bytes);
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            fileChannel.write(buffer);
                        }
                        toProduce = data.poll();
                    }
                    break;
                default:
                    break;
            }
        }

        @Override
        public void run() {
            try {
                int offset = 0;
                while (true) {
                    Queue<Integer> data = queue.take();
                    if (data.peek() == Integer.MAX_VALUE) {
                        System.out.println("数据已全部生产写入完毕");
                        return;
                    }
                    if (method == 1) {
                        int toProduce = data.peek();
                        int length = toProduce == 1 ? 0 : String.valueOf(toProduce - 1).getBytes().length;
                        offset += length * SIZE;
                        setStartPos(toProduce, offset);
                        write(data, method);
                        cyclicBarrier.await();//等待其他线程执行完毕
                    } else if (method == 2 || method == 3) {
                        write(data, method);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}


