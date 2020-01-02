import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author 19028
 * @date 2019/12/8 17:57
 */

public class CAWriter {
    private Cluster cluster;
    private final static int SIZE = 256;
    private final static int MAX_NUM = 2014 * 512;
    private int threadSize;
    private int method;
    private CyclicBarrier cyclicBarrier;


    public static void main(String args[]) throws BrokenBarrierException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("---------------------------请输入写策略-------------------------\n" +
                "|     数字1:RandomAccessFile.write(byte[])（单次写入1024字节）  |\n" +
                "|     数字2:RandomAccessFile.write(byte[])（单次写入2048字节）  |\n" +
                "|     数字3:RandomAccessFile.write(byte[])（单次写入4096字节）  |\n" +
                "--------------------------------------------------------------");
        int method = scanner.nextInt();
        int[] threadSizes = {32, 16, 8, 4, 2, 1};
        for (int threadSize : threadSizes) {
            CAWriter caWriter = new CAWriter(threadSize, method);
            caWriter.start();
        }
    }

    private CAWriter(int threadSize, int method) {
        this.threadSize = threadSize;
        this.method = method;
        this.cluster = Cluster.builder().addContactPoint("47.97.205.133").withPort(9042)
                .withCredentials("dananguo", "Zyc190285975").build();
        this.cyclicBarrier = new CyclicBarrier(threadSize + 1);//加上主线程
    }

    private void createTable() {
        Session session = cluster.connect("mydb");
        String cql = "DROP TABLE" + " data ";
        session.execute(cql);
        cql = "create table data\n" +
                "(\n" +
                "\tid int primary key,\n" +
                "\tdatablock blob\n" +
                ");";
        session.execute(cql);
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

    private void start() throws BrokenBarrierException, InterruptedException {
        createTable();
        int sliceSize = setSliceSize();
        Producer producer = new Producer(SIZE, MAX_NUM, method);
        final long startTime = System.currentTimeMillis();

        //得到装有每个数生成256次的总列表
        List<byte[]> list = producer.produce();
        int startPos = 0;
        Thread thread;
        for (int i = 0; i < threadSize; i++) {
            List<byte[]> slice = list.subList(startPos, sliceSize * (i + 1));
            thread = new Thread(new SliceWriterThread(slice, cyclicBarrier, cluster, i));
            thread.start();
            startPos += sliceSize;
        }
        cyclicBarrier.await();
        System.out.println("总时间开销: " + (System.currentTimeMillis() - startTime));
    }

    static class SliceWriterThread extends Thread {
        private List<byte[]> slice;
        private CyclicBarrier cyclicBarrier;
        private Cluster cluster;
        private int id;

        private SliceWriterThread(List<byte[]> slice, CyclicBarrier cyclicBarrier, Cluster cluster, int id) {
            this.slice = slice;
            this.cyclicBarrier = cyclicBarrier;
            this.cluster = cluster;
            this.id = id * slice.size();
        }

        @Override
        public void run() {
            Session session = cluster.connect("mydb");
            ByteBuffer buffer;
            try {
                int id = this.id;
                for (byte[] bytes : slice) {
                    buffer = ByteBuffer.wrap(bytes);
                    Insert insert = QueryBuilder.insertInto("mydb", "data").
                            value("id", id).
                            value("datablock", buffer);
                    session.execute(insert);
                    id++;
                }
                cyclicBarrier.await();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("插入失败");
            }
            System.out.println("finish");
        }
    }
}
