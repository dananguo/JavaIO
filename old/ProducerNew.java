import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author 19028
 * @date 2019/11/22 21:33
 */

public class ProducerNew implements Runnable {
    private int size;
    private int MAX_NUM;
    private int method;
    private int threadSize;
    private BlockingQueue<Queue<Integer>> queue;
    private CyclicBarrier cyclicBarrier;

    ProducerNew(LinkedBlockingQueue<Queue<Integer>> linkedBlockingQueue, CyclicBarrier cyclicBarrier, int size, int MAX_NUM, int method, int threadSize) {
        this.queue = linkedBlockingQueue;
        this.cyclicBarrier = cyclicBarrier;
        this.size = size;
        this.MAX_NUM = MAX_NUM;
        this.method = method;
        this.threadSize = threadSize;
    }

    @Override
    public void run() {
        try {
            if (method == 2 || method == 3) {
                for (int toProduce = 1; toProduce <= MAX_NUM; toProduce++) {
                    Queue<Integer> buffer = new LinkedList<>();
                    for (int i = 0; i < size; i++) {
                        buffer.add(toProduce);
                    }
                    queue.put(buffer);
                }
            } else if (method == 1) {
                for (int toProduce = 1; toProduce <= MAX_NUM; toProduce++) {
                    int total = 0;
                    while (total < size) {
                        Queue<Integer> buffer = new LinkedList<>();
                        for (int i = 0; i < size / threadSize; i++) {
                            buffer.add(toProduce);
                            total++;
                        }
                        queue.put(buffer);
                    }
                    //这个方法需要同一个数全部写入后才能继续生产下一个数，否则无法计算偏移量
                    cyclicBarrier.await();
                }
            } else {
                throw new IllegalStateException("Unexpected value: " + method);
            }
            for (int i = 0; i < threadSize; i++) {
                Queue<Integer> buffer = new LinkedList<>();
                buffer.add(Integer.MAX_VALUE);
                queue.put(buffer);
            }
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
//        //三种Writer
//        //1. MTWriter
//        MTWriter.isCompleted = true;
//        //2. HDWriter
////        HDWriter.isCompleted = true;
//        //TODO:
    }
}
