package kz.kkb.dbpool;

import javax.servlet.http.HttpServlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * DefaultPoolServlet - Сервлет, который можно использовать в случае если у вас всего одна база данных.
 * В этом случае этот класс необходимо прописать как сервлет в web.xml
 * И вызывать в любом месте приложения как PoolServlet.getConnection() и т.п.
 */
public class PoolServlet extends HttpServlet {

    protected  static ConnectionPool connPool;

    public void init (ServletConfig config) throws ServletException  {
        super.init();
        dbName=config.getInitParameter("dbName");
        dbUser=config.getInitParameter("user");
        dbPassword=config.getInitParameter("password");
        dbDriver=config.getInitParameter("driver");
        try{
            if (connPool==null) {
                connPool=new ConnectionPool(config.getServletName(),dbDriver,dbName,dbUser,dbPassword);
            }
        }catch (Exception e){
            throw new ServletException("PoolServlet cannot start: "+e.toString());
        }
        //устанавливаем параметры пула из web.xml (24.05.2006 по просьбе Олега)
        try {
            connPool.poolData.minSpareConn = Integer.parseInt(config.getInitParameter("minSpareConn"));
        } catch (Exception e) {}
        try {
            connPool.poolData.maxConn = Integer.parseInt(config.getInitParameter("maxConn"));
        } catch (Exception e) {}
        if (null != config.getInitParameter("testQuery"))
            connPool.poolData.sTestQuery=config.getInitParameter("testQuery");
        System.out.println("PoolServlet started.");
    }

    protected String dbName;
    protected String dbUser;
    protected String dbPassword;
    protected String dbDriver;
    protected String dbLogFile;

    public void destroy (){
        connPool.destroy();
        super.destroy();
    }

    public static Connection getConnection() throws SQLException{
        return connPool.getConnection();
    }

    public static void freeConnection(Connection conn){
        connPool.freeConnection(conn);
    }

    public static ConnectionPool getBroker(){
        return connPool;
    }

}
