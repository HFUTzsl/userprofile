/**
 * @Author shanlin
 * @Date Created in  2021/4/1 22:09
 */
public class Test1 {
    final static int VALUE = 10;

    public static void main(String[] args) {
        long l1 = System.currentTimeMillis();

        int i = 0;
        while (System.currentTimeMillis() - l1 < VALUE) {
            i = i + 1;
        }
        System.out.println(i);
    }
}
