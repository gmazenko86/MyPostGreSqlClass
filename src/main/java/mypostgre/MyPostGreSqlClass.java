package mypostgre;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import org.postgresql.ds.PGSimpleDataSource;

import myioutils.MyIOUtils;

public class MyPostGreSqlClass {

    public final String configFilePath;

    public final Connection conn;

    public MyPostGreSqlClass(String configFilePath){
        this.configFilePath = configFilePath;
        this.conn = getConnection();
    }

    public Connection getConnection(){
            return connectUsingDataSource();
    }

    public Connection connectUsingDriverManager(){
        // This is one way to make a connection. Using a DataSource object is preferred
        // per the javax.sql Package documentation
        // https://docs.oracle.com/en/java/javase/14/docs/api/java.sql/javax/sql/DataSource.html
        // Important: The DataSource interface is implemented by a driver vendor.

        // class Properties extends java.util.Hashtable<Object,​Object>
        Properties properties = new Properties();
        StringBuilder stringBuilder = new StringBuilder();

        properties.putAll(convertInputFileToProperties(this.configFilePath));

        // build the URL to the dbase
        stringBuilder.append("jdbc:postgresql://");
        stringBuilder.append(properties.get("hostname"));
        stringBuilder.append("/");
        stringBuilder.append(properties.get("dbname"));
        String dbURL = stringBuilder.toString();

        // now connect to the dbase
        // cannot use try-with-resources or the connection gets closed
        try {
            return DriverManager.getConnection(dbURL, properties);
        } catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    public Connection connectUsingDataSource () {
        // This is the preferred method of making a connection
        // per the javax.sql Package documentation
        // https://docs.oracle.com/en/java/javase/14/docs/api/java.sql/javax/sql/DataSource.html
        // Important: The DataSource interface is implemented by a driver vendor.
        // PGSimpleDataSource implements the DataSource Interface

        Properties properties = new Properties();

        // populate the properties object with the data in the config file
        properties.putAll(convertInputFileToProperties(this.configFilePath));

        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();

        // the properties below are defined in the config file
        pgSimpleDataSource.setServerName(properties.getProperty("hostname"));
        pgSimpleDataSource.setDatabaseName(properties.getProperty("dbname"));
        pgSimpleDataSource.setUser(properties.getProperty("user"));
        pgSimpleDataSource.setPassword(properties.getProperty("password"));
        pgSimpleDataSource.setLoginTimeout(3);

        // cannot use try-with-resources or the connection gets closed
        try {
            return pgSimpleDataSource.getConnection();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public Properties convertInputFileToProperties(String configFilePath){
        // class Properties extends java.util.Hashtable<Object,​Object>
        Properties properties = new Properties();
        String key, value;
        try(Scanner scanner = new Scanner(new File(configFilePath))){
            while(scanner.hasNextLine()){
                key = scanner.next();
                scanner.next(); // skip the equals sign
                value = scanner.next();
                properties.put(key,value);
                scanner.nextLine();
            }
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        }
        return properties;
    }

    public void printScrollableResultSet(ResultSet resultSet){
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int noColumns = rsmd.getColumnCount();
            // want to get the number of rows in the ResultSet
            resultSet.last(); // move cursor to the last row
            int lastRow = resultSet.getRow(); // get the number of rows
            resultSet.first(); // now move the cursor back to the first row

            // print the column headings
            for(int column = 1; column <= noColumns; column++){
                System.out.format("%-15.15s", rsmd.getColumnLabel(column));
                System.out.print("\t");
            }
            System.out.println("\r");

            // print the underlines
            for(int column = 1; column <= noColumns; column++){
                String str = "-";
                int repeatNumber = (rsmd.getColumnLabel(column)).length();
                System.out.format("%-15.15s", str.repeat(repeatNumber));
                System.out.print("\t");
            }
            System.out.println("\r");

            // print the contents of the ResultSet
            for (int row = 0; row < lastRow; row++) {
                for (int column = 1; column <= noColumns; column++) {
                    StringBuilder stringBuilder = new StringBuilder();
                    if (Objects.nonNull (resultSet.getString(column))){
                        stringBuilder.append(resultSet.getString(column));
                    }
                    System.out.format("%-15.15s\t", (stringBuilder.toString()));
                }
                System.out.println("\r");
                resultSet.next();
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    public Statement getStatementScrollable(){
        try {
            // want to make the ResultSet scrollable for printScrollableResultSet()
            // other possible parameters to make the ResultSet scrollable are below
            // Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
            // ResultSet.CONCUR_UPDATABLE);
            return this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    public PreparedStatement getPreparedScrollable(String statestr){
        try {
            return  this.conn.prepareStatement (statestr, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    public void executeQueriesFromFile(String sqlFilePath){
        ArrayList<String> sqlStringArrayList = MyIOUtils.readLinesAsStrings(sqlFilePath);
        removeSqlComments(sqlStringArrayList);
        removeBlankLines(sqlStringArrayList);
        finalizeSqlStatements((sqlStringArrayList));

        String compareString = "select";
        int length = compareString.length();
        int length2;
        boolean beginsWithSelect;
        try(Statement statement = getStatementScrollable()){
            for(String sqlStr : sqlStringArrayList){
                // only execute the queries
                length2 = sqlStr.length();
                String subString = sqlStr.substring(0, Math.min(length, length2));
                beginsWithSelect = subString.equalsIgnoreCase(compareString);
                if(beginsWithSelect) {
                    ResultSet resultSet = statement.executeQuery(sqlStr);
                    MyIOUtils.printYellowText("SQL statement: " + sqlStr + " produces the following result:");
                    System.out.println();
                    printScrollableResultSet(resultSet);
                    resultSet.close();
                }
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    void removeSqlComments(ArrayList<String> arrayList){
        ArrayList<String > swapList = new ArrayList<>();
        for(String str: arrayList){
            // do not keep sql comment lines
            if(!str.startsWith("--")){
                swapList.add(str);
            }
        }
        arrayList.clear();
        arrayList.addAll(swapList);
    }

    void removeBlankLines(ArrayList<String> arrayList){
        ArrayList<String > swapList = new ArrayList<>();
        for(String str: arrayList){
            // do not keep blank lines
            if(!str.isEmpty()){
                swapList.add(str);
            }
        }
        arrayList.clear();
        arrayList.addAll(swapList);
    }

    void finalizeSqlStatements(ArrayList<String> arrayList){
        ArrayList<String > swapList = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder();
        for(String str: arrayList){
            // consolidate multi line statements into one ArrayList entry
            // start the StringBuilder with the first line of the sql command
            stringBuilder.append(str);
            if(str.endsWith(";")){
                // save it to the swapList if it ends with a ;
                swapList.add(stringBuilder.toString());
                // reset the StringBuilder
                stringBuilder.setLength(0);
            } else{
                stringBuilder.append("\n");
            }
        }
        arrayList.clear();
        arrayList.addAll(swapList);
    }

    void printConnectionStatus(Connection connection) {
        try{
            System.out.println("From ConnectionDemo.runDemo(): " + connection +
                    " is closed = " + connection.isClosed());
        } catch (SQLException exception){
            exception.printStackTrace();
        }
    }

    public void closeConnection(Connection connection){
        try{
            connection.close();
        } catch(SQLException exception){
            exception.printStackTrace();
        }
    }

}
