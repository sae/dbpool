package kz.kkb.dbpool;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
import java.util.Set;

/**
 * Создаватель коннекций.
 * Проверяет количество свободных коннекций и создает запасные.
 * Процесс создания коннекции может зависать.
 * В этом случае объект можно убить и создать новый.
 */
public class ConnCreator  implements Runnable {


    private Thread runner;//процесс, поддерживающий пул
    private PoolData poolData;
    private Logger logger;

    public ConnCreator(PoolData pd) {
        poolData=pd;
        logger=poolData.logger;
    }
    /**
     * Создает новую проверенную коннекцию.
     * НЕ добавляет коннекцию в пул
     * @return
     * @throws java.sql.SQLException если создана неправильная коннекция
     */

    private Connection newConn() throws SQLException {
      logger.debug("Creating a new connection "+poolData.URL+"...");
      Connection con = null;
      long lTime=System.currentTimeMillis();
      DriverManager.setLoginTimeout(20);//таймаут на логин в базу
      con = DriverManager.getConnection(poolData.URL, poolData.user, poolData.password);
      //тестируем полученную коннекцию
      if (!testConn(con)) {
            logger.error ("Connection ERROR !");
            throw new SQLException("Cannot create connection");
      }
      logger.debug("Created a new connection "+poolData.connName(con)+" in "+(System.currentTimeMillis()-lTime)+" ms");
      return con;
    }

    //public void start() {
    //    runner = new Thread(this);
    //    runner.start();
    //}
    //public void stop() {
        //runner.interrupt();
        //try { runner.join(10000); }
        //catch(InterruptedException e){} // ignore
    //}

    /**
     * тестирует коннекцию
     * @return
     */
    public boolean testConn(Connection con) {
        boolean rc=true;
        String s="conn is null";
        //если null, тест failed
        if (con == null) {
            logger.error("Connection is null - test failed");
            return false;
        }
        //иначе тестируем
        try {
            if (con.isClosed()) {
                s="conn closed";
                rc=false;
            }
            //isClosed не тестирует соединение !
            //приходится тестировать реальным селектом
            con.createStatement().execute(poolData.sTestQuery);
        } catch (Exception e) {
            s=e.toString();
            rc=false;
        }
        //если тест не прошел - ошибка
        if (!rc) logger.error("Connection "+poolData.connName(con)+" test failed: "+s);
        return rc;
    }

    public void run() {
        logger.info("ConnCreator thread started");
        while(true) {
            try {//prevent from exit by exception
                //set current time for checking
                poolData.lastConnCreatorTime=System.currentTimeMillis();
                //killing of bad connections
                Object[] entries=poolData.getLeasedTable().keySet().toArray();
                for (int i=0; i<entries.length; i++) {
                    try {
	                	Connection conn=(Connection)entries[i];
	                    Date connDate=(Date)poolData.getLeasedTable().get(conn);
	                    //test for timeout
	                    if (System.currentTimeMillis()-connDate.getTime() > poolData.connTimeout) {
	                        //kill bad connection
	                        logger.warn("Lease timeout! killing connection "+poolData.connName(conn)+", debug="+poolData.getDebugInfo(conn));
	                        poolData.removeFromLeased(conn);
	                        	conn.close();
	                    }
	                } catch (Exception e1) {//catch exception here, for continue if any error occured
	                	logger.error(e1.getMessage(),e1);
	                	//if this closing is in error, don't try another closing
	                }
                }

                if (poolData.getFreeCount() > poolData.minSpareConn*2) {
                    //removing spare connections
                    for (int i=0;i<poolData.getFreeCount() - poolData.minSpareConn;i++) {
                        Connection conn=(Connection)poolData.getFreeConn();
                        conn.close();
                        logger.debug("Spare connection removed "+poolData.connName(conn));
                        }
                    }

                if (poolData.getFreeCount() < poolData.minSpareConn) { //create new spare connections
                    //only if total quantity is less than maxConn
                    if (poolData.getLeasedCount()+poolData.getFreeCount() < poolData.maxConn)
                    //creating one half of difference
                    for (int i=0;i<(poolData.minSpareConn-poolData.getFreeCount())/2;i++) {
                        Connection conn=newConn();
                        poolData.addConnToFree(conn);
                        logger.info("Spare connection created "+poolData.connName(conn));
                        Thread.yield();//временно отдаем управление системе
                        if (runner.interrupted()) {//проверяем на прерывание каждый цикл
                            logger.info("ConnCreator thread interrupted");
                            return;
                        }
                    }
                }
            } catch (Exception e) {//тут ловим возможные ошибки SQL
            	logger.error(e.getMessage(),e);
            }
            //и только потом обрабатываем прерывание и ждем 
            if (runner.interrupted()) {
                logger.info("ConnCreator thread interrupted");
                return;
            }
            // Wait 10 seconds for next cycle
            try {
                Thread.sleep(10000);
            } catch(InterruptedException e) {
                logger.info("ConnCreator thread stopped");
                return;
            }
        }
    }

}
