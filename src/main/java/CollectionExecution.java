import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * @Author: xu.dm
 * @Date: 2019/7/6 21:06
 * @Description:
 */
public class CollectionExecution {
    /**
     * POJO class representing a user.
     */
    public static class User {
        public int userIdentifier;
        public String name;

        public User() {}

        public User(int userIdentifier, String name) {
            this.userIdentifier = userIdentifier; this.name = name;
        }

        public String toString() {
            return "User{userIdentifier=" + userIdentifier + " name=" + name + "}";
        }
    }

    /**
     * POJO for an EMail.
     */
    public static class EMail {
        public int userId;
        public String subject;
        public String body;

        public EMail() {}

        public EMail(int userId, String subject, String body) {
            this.userId = userId; this.subject = subject; this.body = body;
        }

        public String toString() {
            return "eMail{userId=" + userId + " subject=" + subject + " body=" + body + "}";
        }

    }

    public static void main(String args[]) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        User[] usersArray = {
                new User(1, "Peter"),
                new User(2, "John"),
                new User(3, "Bill") };

        EMail[] emailsArray = {
                new EMail(1, "Re: Meeting", "How about 1pm?"),
                new EMail(1, "Re: Meeting", "Sorry, I'm not availble"),
                new EMail(3, "Re: Re: Project proposal", "Give me a few more days to think about it.")};

        DataSet<User> users = env.fromElements(usersArray);
        DataSet<EMail> emails =env.fromElements(emailsArray);

        DataSet<Tuple2<User,EMail>> joined = users.join(emails)
                .where("userIdentifier").equalTo("userId");

        System.out.println("DataSet: ");
        joined.print();

        // retrieve the resulting Tuple2 elements into a ArrayList.
        List<Tuple2<User,EMail>> result = joined.collect();

        System.out.println("List: ");
        for(Tuple2<User,EMail> tuple2: result){
            System.out.println(tuple2);
        }

    }
}
