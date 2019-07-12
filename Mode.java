
/*
 * Mode.java
 *
 * Version:
 *    $id: 1.8.0_144
 *
 * Revision:
 *      11/05/2018
 */
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;

/**
 * The program computes the mode
 *
 * @author     Joshua Randolph
 */

public class CustomMode implements org.h2.api.AggregateFunction{

    private Connection conn;
    private LinkedHashMap<String,Integer>frequency = new LinkedHashMap();
    private String key = null;
    
    /**
     * The aggregrate function is designed to be called.
     * I am not completely sure what this function does.
     * It seems that one could use command: CREATE AGGREGATE MODE FOR "mode"
     */
    public void init(Connection conn) throws SQLException {
    }

    public int getType(int[] ints) throws SQLException {
       return java.sql.Types.VARCHAR;
    }

    /**
     * Add the object into the frequency table
     * @param Object o
     * @throws SQLException
     */
    public void add(Object o) throws SQLException{
        try{
            frequency.put(o.toString(),frequency.get(o.toString())+1);
        } catch(Exception e) {
            frequency.put(o.toString(),1);
        }
    }

    /**
     * Compute the mode
     * @throws SQLException
     * @return the mode
     */
    public Object getResult() throws SQLException{
        String answer = "";
        int count = 0;
        if (frequency.size() > 0){
            Iterator itr = frequency.keySet().iterator();
            while(itr.hasNext()){
                String key = (String)itr.next();
                int num = frequency.get(key);
                if (num > count){
                    answer = key;
                    count = num;
                }
            }
            return answer;
        } else {
            return null;
        }
    }
    
    public static void main(String [] args){
        try{
            Class.forName("org.h2.Driver"); 
            Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test", "sa", "");
            Statement stmt = conn.createStatement();
            String query = "CREATE AGGREGATE MODE FOR \"mode\"";
            System.out.println(query);
            stmt.execute("CREATE AGGREGATE MODE FOR \"Mode\"");
            
        }catch(Exception e){
            System.out.println(e);
        }
    }
}