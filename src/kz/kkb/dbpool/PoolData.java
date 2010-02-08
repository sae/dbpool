package kz.kkb.dbpool;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Date;
import java.sql.Connection;

/**
 * Содержит набор данных для данного пула.
 * Все публичные счетчики и параметры.
 */
public class PoolData {
    public Logger logger; //весь лог пула пишется в один логгер
    public long lastConnCreatorTime; //время последнего срабатывания ConnCreator, проверяется от его зависания
    public int lastNewConnTime; //время последнего создания коннекции, ms
    public String URL;
    public String user;
    public String password;
    public String driverName;
    public long connTimeout=60*1000; //60 seconds
    public ConnectionPool dbpool;//указатель на свой пул
    public String sTestQuery="SELECT 1"; //запрос для тестов  (MySQL и MSSQL , oracle requires FROM DUAL)
    /** указывает выдавать ли лог */
    public boolean isLog=true;
    /** minimum spare connections, default=6. NOTE! actual free connections varies form min to min*2 */
    public int minSpareConn=6;
    /** maximum connections, default=50 */
    public int maxConn=50;

    private ArrayList freeConnections=new ArrayList();//пул свободных коннекций
    private Hashtable leasedConnections=new Hashtable();//пул выданных коннекций
    // Отладочная информация использкется для опроса какие коннекции выполняют долгие запросы или висят.
    private Hashtable leasedDebugInfo=new Hashtable();//сюда ложим отладочную информацию о выданных коннекциях
    public boolean available=true;//флаг разрешения на выдачу коннекций
    public ConnManager connManager;
    public ConnCreator connCreator;
    public ConnectionPool connPool;
    public int hangCounter=0;
    public StringBuffer hangLog=new StringBuffer();
    /**
     * Adding connection to "free table". Connection is not tested.
     * @param conn
     */
    public void addConnToFree(Connection conn) {
        synchronized (freeConnections) {
            freeConnections.add(conn);
        }
    }

    /**
     * Get free connections count
     * @return
     */
    public int getFreeCount() {
        int i=0;
        synchronized (freeConnections) {
            i=freeConnections.size();
        }
        return i;
    }

    /**
     * Put connection into "leased table", add debug info
     * @param conn
     * @param sDebugInfo
     */
    public void addConnToLeased(Connection conn,String sDebugInfo) {
        leasedConnections.put(conn,new Date());
        if (sDebugInfo!=null) leasedDebugInfo.put(conn.toString(),sDebugInfo);
    }

    /**
     * Remove connection from "leased table", debug info is also deleted
     * @param conn
     */
    public void removeFromLeased(Connection conn) {
        leasedConnections.remove(conn);
        leasedDebugInfo.remove(conn.toString());
    }

    /**
     * Obtain "leased table". Warning! don't make direct modifications in table!
     * @return
     */
    public Hashtable getLeasedTable() {
        return leasedConnections;
    }

    /**
     * Get leased connections count
     * @return
     */
    public int getLeasedCount() {
        return leasedConnections.size();
    }

    /**
     * return time of leasing for this connection.
     * return 0 if no information available
     * @param conn
     * @return
     */
    public long getLeaseTime(Connection conn) {
        Date d=(Date)leasedConnections.get(conn);
        if (d==null) return 0;
        return d.getTime();
    }
    /**
     * Get debug info for leased connection.
     * Return null if no debug info available or connection is not in leased table
     * @param conn
     * @return
     */
    public String getDebugInfo(Connection conn) {
        return ""+leasedDebugInfo.get(conn.toString());
    }

    /**
     * Get free connection from pool. Do not add conn to "leased table".
     * Conenction will be added to leased after testing in ConnCreator.
     * Test can hangs, so it placed in ConnCreator.
     * @return
     */
    public Connection getFreeConn() {
        Connection conn=null;
        synchronized (freeConnections) {
            if (!freeConnections.isEmpty()) {
                conn = (Connection) freeConnections.get(freeConnections.size()-1);
                freeConnections.remove(conn);
            } else {
                logger.info("Pool is empty");
            }
        }
        return conn;
    }

    /**
     * additional method for debugging
     * @param conn
     * @return
     */
    public String connName(Connection conn) {
        //String sName=conn.toString();
        //return sName.replaceFirst(conn.getClass().getCanonicalName(),"");
        return ""+conn.hashCode();
    }
}
