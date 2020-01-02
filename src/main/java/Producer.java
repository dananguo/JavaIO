import java.util.ArrayList;
import java.util.List;

/**
 * @author 19028
 * @date 2019/11/22 21:33
 */

public class Producer {
    private int size;
    private int MAX_NUM;
    private int method;
    private List<byte[]> data = new ArrayList<>();

    Producer(int size, int MAX_NUM, int method) {
        this.size = size;
        this.MAX_NUM = MAX_NUM;
        this.method = method;
    }

    private byte[] intToByteArray(int value) {
        byte[] src = new byte[4];
        src[0] = (byte) ((value >> 24) & 0xFF);
        src[1] = (byte) ((value >> 16) & 0xFF);
        src[2] = (byte) ((value >> 8) & 0xFF);
        src[3] = (byte) (value & 0xFF);
        return src;
    }

    public List<byte[]> produce() {
        switch (method) {
            case 1: {
                //每份数据为1024字节
                for (int toProduce = 1; toProduce <= MAX_NUM; toProduce++) {
                    int curPos = 0;
                    byte[] bytes = new byte[size * 4];
                    byte[] eachArray = intToByteArray(toProduce);
                    for (int i = 0; i < size; i++) {
                        System.arraycopy(eachArray, 0, bytes, curPos, eachArray.length);
                        curPos += 4;
                    }
                    data.add(bytes);
                }
                break;
            }
            case 2: {
                //每份数据为2048字节
                int toProduce = 1;
                while (toProduce <= MAX_NUM) {
                    int curPos = 0;
                    byte[] bytes = new byte[size * 4 * 2];
                    for (int i = 0; i < 2; i++) {
                        byte[] eachArray = intToByteArray(toProduce);
                        for (int j = 0; j < size; j++) {
                            System.arraycopy(eachArray, 0, bytes, curPos, eachArray.length);
                            curPos += 4;
                        }
                        toProduce++;
                    }
                    data.add(bytes);
                }
                break;
            }
            case 3: {
                //每份数据为4096字节
                int toProduce = 1;
                while(toProduce <= MAX_NUM) {
                    int curPos = 0;
                    byte[] bytes = new byte[size * 4 * 4];
                    for (int i = 0; i < 4; i++) {
                        byte[] eachArray = intToByteArray(toProduce);
                        for (int j = 0; j < size; j++) {
                            System.arraycopy(eachArray, 0, bytes, curPos, eachArray.length);
                            curPos += 4;
                        }
                        toProduce++;
                    }
                    data.add(bytes);
                }
                break;
            }
        }
        return data;
    }
}
