
import mypostgre.MyPostGreSqlClass;
//import org.apache.commons.io.output.NullPrintStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;

public class MyPostGreSqlTest {

    @Test
    void testgetConnection() {

        String configFilePath = "src/main/resources/config.txt";

        MyPostGreSqlClass dbMgr = new MyPostGreSqlClass(configFilePath);
        boolean connectionValid = false;
        try(Connection conn = dbMgr.getConnection()){
            connectionValid = conn.isValid(2);
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        Assertions.assertTrue(connectionValid, "Dbase connection not established within timeout");
    }
/*
    void testNullPrintStream(){
        System.out.println("Message is shown");
        PrintStream original = System.out;
        System.setOut(new NullPrintStream());
        System.out.println("Message not shown");
        System.setOut(original);
        System.out.println("First message after 'Message is shown' to standard console?");
    }

 */
}
