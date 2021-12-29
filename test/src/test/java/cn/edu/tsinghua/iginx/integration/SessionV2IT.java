package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.session_v2.*;
import cn.edu.tsinghua.iginx.session_v2.domain.User;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SessionV2IT {

    private static IginXClient iginXClient;

    private static WriteClient writeClient;
    private static AsyncWriteClient asyncWriteClient;

    private static DeleteClient deleteClient;

    private static QueryClient queryClient;

    private static UsersClient usersClient;

    private static ClusterClient clusterClient;

    @BeforeClass
    public static void setUp() {
        iginXClient = IginXClientFactory.create("127.0.0.1", 6888);

        writeClient = iginXClient.getWriteClient();
        asyncWriteClient = iginXClient.getAsyncWriteClient();
        deleteClient = iginXClient.getDeleteClient();
        queryClient = iginXClient.getQueryClient();
        usersClient = iginXClient.getUserClient();
        clusterClient = iginXClient.getClusterClient();

        insertData();
    }

    @AfterClass
    public static void tearDown() {
        clearDataAndUser();
        iginXClient.close();
    }

    private static void insertData() {
        Table.Builder builder = Table.builder().measurement("test.session.v2")
                .addField("bool", DataType.BOOLEAN)
                .addField("int", DataType.INTEGER)
                .addField("long", DataType.LONG)
                .addField("float", DataType.FLOAT)
                .addField("double", DataType.DOUBLE)
                .addField("string", DataType.BINARY);

        long startTimestamp = 0L;
        long endTimestamp = 15000L;

        for (long i = startTimestamp; i < endTimestamp; i++) {
            builder = builder.timestamp(i)
                    .boolValue("bool", i % 2 == 0 ? true : false)
                    .intValue("int", 1000)
                    .longValue("long", 232333L)
                    .floatValue("float", (float) (i + 0.1))
                    .doubleValue("double", i + 0.2)
                    .binaryValue("string", String.valueOf(i).getBytes())
                    .next();
        }

        Table table = builder.build();
        writeClient.writeTable(table);
    }

    private static void clearDataAndUser() {
        List<User> users = usersClient.findUsers();
        users.forEach(user -> {
            if (!user.getUsername().equals("root")) {
                usersClient.removeUser(user.getUsername());
            }
        });
        deleteClient.deleteMeasurement("*");
    }

    @Test
    public void testQueryClient() {

    }

    @Test
    public void testUserClient() {
        Set<AuthType> fullAuth = new HashSet<>(Arrays.asList(AuthType.Read, AuthType.Write, AuthType.Admin, AuthType.Cluster));
        Set<AuthType> rdsAuth = new HashSet<>(Arrays.asList(AuthType.Read, AuthType.Write, AuthType.Admin));
        Set<AuthType> rdAuth = new HashSet<>(Arrays.asList(AuthType.Read, AuthType.Write));

        List<User> actualUsers = usersClient.findUsers();
        List<User> expectedUsers = new ArrayList<>(
                Collections.singletonList(
                        new User("root", "root", UserType.Administrator, fullAuth)
                )
        );
        assertEqualUsers(expectedUsers, actualUsers);

        // test add user
        User user1 = new User("user1", "user1", UserType.OrdinaryUser, rdAuth);
        User user2 = new User("user2", "user2", UserType.OrdinaryUser, rdsAuth);
        usersClient.addUser(user1);
        usersClient.addUser(user2);

        expectedUsers.add(user1);
        expectedUsers.add(user2);
        actualUsers = usersClient.findUsers();
        assertEqualUsers(expectedUsers, actualUsers);

        // test update user
        expectedUsers.remove(user2);

        user2 = new User("user2", "user2", UserType.OrdinaryUser, fullAuth);
        usersClient.updateUser(user2);

        expectedUsers.add(user2);
        actualUsers = usersClient.findUsers();
        assertEqualUsers(expectedUsers, actualUsers);

        // test remove user
        usersClient.removeUser(user1.getUsername());

        expectedUsers.remove(user1);
        actualUsers = usersClient.findUsers();
        assertEqualUsers(expectedUsers, actualUsers);
    }

    private void assertEqualUsers(List<User> users1, List<User> users2) {
        if (!isEqualUsers(users1, users2)) {
            fail();
        }
    }

    private boolean isEqualUsers(List<User> users1, List<User> users2) {
        if (users1 == null) return users2 == null;
        if (users2 == null) return false;
        if (users1.size() != users2.size()) return false;

        for (User user1 : users1) {
            boolean isMatch = false;
            for (User user2: users2) {
                if (user1.getUsername().equals(user2.getUsername())) {
                    isMatch = true;
                    if (!isEqualUser(user1, user2)) {
                        return false;
                    }
                }
            }
            if (!isMatch) {
                return false;
            }
        }
        return true;
    }

    private boolean isEqualUser(User user1, User user2) {
        if (user1 == null) return user2 == null;
        if (user2 == null) return false;

        if (!user1.getUsername().equals(user2.getUsername()))
            return false;
        if (!user1.getUserType().equals(user2.getUserType()))
            return false;

        return user1.getAuths().containsAll(user2.getAuths()) &&
                user2.getAuths().containsAll(user1.getAuths());
    }
}
