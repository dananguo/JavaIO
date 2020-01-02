import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author 19028
 * @date 2019/11/22 21:33
 */

public class Producer implements Runnable {
    private int size;
    private int MAX_NUM;
    private BlockingQueue<Integer> queue;
    private CyclicBarrier cyclicBarrier;

    Producer(ArrayBlockingQueue<Integer> arrayBlockingQueue, CyclicBarrier cyclicBarrier, int size, int MAX_NUM) {
        this.queue = arrayBlockingQueue;
        this.cyclicBarrier = cyclicBarrier;
        this.size = size;
        this.MAX_NUM = MAX_NUM;
    }

    @Override
    public void run() {
        for (int toProduce = 1; toProduce <= MAX_NUM; toProduce++) {
            try {
                for (int i = 0; i < size; i++) {
                    queue.put(toProduce);
                }
//                long currentTime = System.currentTimeMillis();
//                System.out.println(size + "个" + toProduce + "已生产完毕, 时间开销: " + (System.currentTimeMillis() - currentTime));
                if (toProduce < MAX_NUM)
                    cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
        MTWriter.isCompleted = true;
    }
}
