import java.sql.Connection;
import java.sql.SQLException;

public class MyPostGreSqlTest {

    static public void main(String... varargs){
        String configFilePath = "src/test/config.txt";
        MyPostGreSqlClass dbMgr = new MyPostGreSqlClass(configFilePath);
        try(Connection conn = dbMgr.getConnection()){
            System.out.println(conn.toString() + " is closed = " + conn.isClosed());
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }
}
