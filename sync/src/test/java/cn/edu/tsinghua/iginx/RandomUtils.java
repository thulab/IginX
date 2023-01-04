package cn.edu.tsinghua.iginx;

import java.util.Random;

public class RandomUtils {

    private static final Random random = new Random();

    public static String randomString(int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int rand = random.nextInt(26 * 2 + 10);
            if (rand < 10) {
                builder.append(rand);
                continue;
            }
            rand -= 10;
            if (rand < 26) {
                builder.append((char)('A' + rand));
                continue;
            }
            rand -= 26;
            builder.append((char)('a' + rand));
        }
        return builder.toString();
    }

    public static int randomNumber(int lowBound, int upBound) {
        return random.nextInt(upBound - lowBound) + lowBound;
    }

    public static boolean randomTest(double chance) {
        return random.nextDouble() < chance;
    }

}
