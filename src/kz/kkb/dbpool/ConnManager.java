package kz.kkb.dbpool;

import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Date;
import java.sql.Connection;

/**
 * Процесс, управляющий освобождением коннекций и удалением лишних.
 */
public class ConnManager  implements Runnable {

    private Thread runner;//процесс, поддерживающий пул
    private Thread creator_runner;//процесс, создающий коннекции
    private PoolData poolData;
    private Logger logger;

    public ConnManager(PoolData pd) {
        poolData=pd;
        logger=poolData.logger;
    }
    public void start() {
        poolData.lastConnCreatorTime=System.currentTimeMillis();
        startCreator();
        runner = new Thread(this);
        runner.start();
    }

    public void stop() {
        stopCreator();
        runner.interrupt();
        // Wait until the housekeeping thread has died.
        //try { runner.join(5000); }
        //catch(InterruptedException e){} // ignore
    }

    private void startCreator() {
        poolData.connCreator=new ConnCreator(poolData);
        creator_runner = new Thread(poolData.connCreator);
        creator_runner.start();
    }

    private void stopCreator() {
        creator_runner.interrupt();
        // Wait until the housekeeping thread has died.
        //try { creator_runner.join(10000); }
        //catch(InterruptedException e){} // ignore
    }
    /**
     * Процесс, поддерживающий пул.
     * В некоторых случаях тред зависает, похоже при создании новой коннекции,
     * поэтому менеджер может рестартовать тред создания коннекций
     */
    public void run() {
        logger.info("Manager thread started");
        while(true) {
            try {//prevent from exit by exception
                long currTime=System.currentTimeMillis();
                //отстрел зависшего креатора
                if (currTime-poolData.lastConnCreatorTime>60000*5) { //if hangs more than 5 minutes ago
                    //тред завис, надо пересоздать
                    logger.error("Creator is hangs, starting new...");
                    //добавляем диагностику зависаний
                    poolData.hangCounter++;
                    poolData.hangLog.append ("Hangs at "+new Date()+"\n");
                    if (poolData.hangLog.length()>1000) {
                        poolData.hangLog.delete(0,1000-poolData.hangLog.length());
                        poolData.hangLog.trimToSize();
                    }
                    //стопим зависший тред обязательно - иначе он будет продолжать работать с PoolData 
                    stopCreator();
                    startCreator();
                }
            } catch (Exception e) {
                logger.error("Manager error: ",e);
            }
            // Wait 20 seconds for next cycle
            try {
                Thread.sleep(20000);
            } catch(InterruptedException e) {
                // Returning from the run method sets the internal
                // flag referenced by Thread.isAlive() to false.
                // This is required because we don't use stop() to
                // shutdown this thread.
                logger.info("Manager thread stopped");
                return;
            }
        }
    }
}
