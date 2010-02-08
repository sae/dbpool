package kz.kkb.dbpool;

import org.apache.log4j.Logger;

import java.sql.DriverManager;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Date;

/**
 * <pre>
 * Пул коннекций.
 * Преимущества:
 * - поддерживает minSpareConn запасных коннекций (по умолчанию 10)
 * - имеет ограничение на число открытых коннекций maxConn (100)
 * - препятствует взрывообразному росту коннекций, т.к. тред, открывающий коннекции выполнен отдельно.
 * - зависание треда открытия коннекций не является проблемой: все остальное работает в другомм треде
 * - тред открытия коннекций при зависании автоматически пересоздается
 * - логгирование идет через log4j
 *  
 * Создавать по одному пулу для каждой базы данных
 * Пул создается как объект. Затем он порождает внутри себя объект ConnManager,
 * передает ему параметры и запускает его тред.
 *
 * ConnManager в свою очередь управляет тредом объекта ConnCreator.
 * Т.е. проверяет что тред не завис, и если завис, пересоздает объект ConnCreator
 *
 * Логирование выполняется с помощью log4j.
 * Имя логгера: ConnectionPool.{poolName}
 * log4j.properties помещать в /WEB_INF/classes
 */

 /*
 * 11-aug-2006
 * Добавлено логирование номера коннекции
 * Теперь не создаются коннекции если суммарное их число больше maxConn
 *
 * 10-aug-2006
 * Добавлены методы в PoolData по синхронной работе с FreeConnections
 * 
 * </pre>
 */
public class ConnectionPool {// implements Runnable{

    public PoolData poolData=new PoolData();
    private Logger logger;

    /**
     * Процедура установки имени логгера log4j.
     * @param name
     */
    protected void setLogger(String name) {
        logger=Logger.getLogger("ConnectionPool."+name);
        poolData.logger=logger;
    }

    /**
     * Создает пул коннекций и запускает процесс поддержки
     * @param poolName
     * @param driver
     * @param URL
     * @param user
     * @param password
     */
    public ConnectionPool(String poolName, String driver, String URL, String user, String password) {
      setLogger(poolName);
      poolData.connPool=this;
      poolData.driverName=driver;
      poolData.URL = URL;
      poolData.user = user;
      poolData.password = password;
      try {
        Driver dbdriver = (Driver)Class.forName(poolData.driverName).newInstance();
        DriverManager.registerDriver(dbdriver);
        //oracle required FROM statement
        if (poolData.driverName.indexOf("oracle")>=0) poolData.sTestQuery="SELECT 1 FROM DUAL";
        logger.debug("Registered JDBC driver: "+poolData.driverName);
      }
      catch (Exception e) {
        logger.error("Can't register JDBC driver: "+poolData.driverName, e);
      }

        poolData.connManager=new ConnManager(poolData);
        poolData.connManager.start();
    }

    /**
     * Гарантированно выдает исправную коннекцию.
     * Либо из пула, либо новую.
     * Если выдача коннекции невозможна, генерирует SQLException.
     * @return
     * @throws java.sql.SQLException если невозможно создать правильную коннекцию
     */
    public Connection getConnection() throws SQLException {
        String sDebug=new Exception().getStackTrace()[2].toString();
        return getConnection(sDebug);
    }

    public Connection getConnection(String sDebugInfo) throws SQLException {
        //disable connection lease
        if (!poolData.available)
            throw new SQLException("Cannot establish connection");
        //if maximum connections leased throw exception
        if (poolData.getLeasedCount()>poolData.maxConn) {
            logger.error("Maximum leased connections reached"+", pool size is "+poolData.getFreeCount()
                   +", leased "+poolData.getLeasedCount());
            throw new SQLException("Maximum leased connections reached");
        }
        Connection con=null;
        for (int i=0; i<poolData.maxConn && con==null ;i++) { //ограничим цикл поиска коннекции
            //if (freeConnections.isEmpty()) throw new SQLException("Pool is empty");
            //получаем из пула (без тестирования)
            con = getConn();
            if (con==null) break; //если из пула не выдана коннекция, то нет смысла спрашивать еще
            if (!poolData.connCreator.testConn(con)) con=null; //если тест не прошел - выбрасываем
            //если пул пуст, получаем новую коннекцию (тестируем внутри)
            //if (con == null) con = newConn();
        }
        if (con==null) throw new SQLException("Cannot obtain connection from pool");
        //переводим коннекцию в список выданных, добавляем таймстамп
        poolData.addConnToLeased(con,sDebugInfo);

        logger.debug("Lease connection "+poolData.connName(con)+", pool size is "+poolData.getFreeCount()
                +", leased "+poolData.getLeasedCount());
        return con;
    }

    /**
     * Возвращает непроверенную коннекцию или null если пул пуст
     * @return
     */
    private Connection getConn() {
        return poolData.getFreeConn();
    }

    /**
     * Return the connection to free pool. In case of error, it suppressed and warning generated.
     * @param con
     */
    public synchronized void freeConnection(Connection con) {
        try {
            Date d=(Date)poolData.getLeasedTable().get(con);
            String debug=(String)poolData.getDebugInfo(con);
            poolData.removeFromLeased(con);//удаляем коннекцию из списка выданных
            poolData.addConnToFree(con);
            logger.debug("Return connection "+poolData.connName(con)+" to pool"+", pool size is "+poolData.getFreeCount()
                      +", leased "+poolData.getLeasedCount());
            long conn_time=System.currentTimeMillis()-d.getTime();
            if (conn_time>15000)
                logger.warn("Connection "+poolData.connName(con)
                    + " use_time="+conn_time+" ms"
                    + ", debug_info="+debug);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }

    /**
     * Завершает работу пула - закрывает все коннекции и очищает пул.
     * Останавливает процесс поддержки.
     */
    public void destroy() {
        // Stop issuing connections
        poolData.available=false;
        // Shut down the background housekeeping thread
        poolData.connManager.stop();
        Iterator allConnections;

        while (poolData.getFreeCount()>0) {
            Connection con = poolData.getFreeConn();
            try {
              con.close();
              logger.debug("Closed connection for pool ");
            }
            catch (SQLException e) {
              logger.debug("Can't close connection for pool ");
            }
        }

        //очищаем занятые коннекции
        allConnections = poolData.getLeasedTable().keySet().iterator();
        while (allConnections.hasNext()) {
          Connection con = (Connection) allConnections.next();
          try {
            con.close();
            logger.debug("Closed leased connection for pool ");
          }
          catch (SQLException e) {
            logger.debug("Can't close leased connection for pool ");
          }
        }
        poolData.getLeasedTable().clear();
    }

    /**
     * Выдает текущее количество свободных коннекций
     * @return
     */
    public int getFreeCount() {
        return poolData.getFreeCount();
    }
    
    /**
     * Выдает текущее количество занятых коннекций
     * @return
     */
    public int getLeasedCount() {
        return poolData.getLeasedCount();
    }

}
