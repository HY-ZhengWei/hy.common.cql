package org.hy.common.xcql;

import java.util.Map;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;





/**
 * 将第三方的Connection对象二次封装，统一监控接口
 *
 * @author      ZhengWei(HY)
 * @createDate  2017-07-13
 * @version     v1.0
 */
public class Connection implements Session
{
    
    /** 所属的数据库连接信息 */
    private DataSourceCQL       dataSourceCQL;
    
    /** 第三方的连接对象实例 */
    private Session             conn;
    
    
    
    public Connection(final Session i_Connection ,final DataSourceCQL i_DataSourceCQL)
    {
        this.conn          = i_Connection;
        this.dataSourceCQL = i_DataSourceCQL;
    }
    

    
    /**
     * 获取：所属的数据库连接信息
     */
    public DataSourceCQL getDataSourceCQL()
    {
        return dataSourceCQL;
    }
    

    
    /**
     * 获取：第三方的连接对象实例
     */
    public Session getConnection()
    {
        return conn;
    }
    
    
    
    @Override
    public void close()
    {
        this.conn.close();
        
        if ( this.dataSourceCQL != null )
        {
            this.dataSourceCQL.connClosed();
        }
    }



    @Override
    public boolean isOpen()
    {
        return this.conn.isOpen();
    }



    @Override
    public Result run(String query ,Value parameters)
    {
        return this.conn.run(query ,parameters);
    }



    @Override
    public Result run(String query ,Map<String ,Object> parameters)
    {
        return this.conn.run(query ,parameters);
    }



    @Override
    public Result run(String query ,Record parameters)
    {
        return this.conn.run(query ,parameters);
    }



    @Override
    public Result run(String query)
    {
        return this.conn.run(query);
    }



    @Override
    public Result run(Query query)
    {
        return this.conn.run(query);
    }



    @Override
    public Transaction beginTransaction()
    {
        return this.conn.beginTransaction();
    }



    @Override
    public Transaction beginTransaction(TransactionConfig config)
    {
        return this.conn.beginTransaction(config);
    }



    @Override
    public <T> T readTransaction(TransactionWork<T> work)
    {
        return this.conn.readTransaction(work);
    }



    @Override
    public <T> T readTransaction(TransactionWork<T> work ,TransactionConfig config)
    {
        return this.conn.readTransaction(work ,config);
    }



    @Override
    public <T> T writeTransaction(TransactionWork<T> work)
    {
        return this.conn.writeTransaction(work);
    }



    @Override
    public <T> T writeTransaction(TransactionWork<T> work ,TransactionConfig config)
    {
        return this.conn.writeTransaction(work ,config);
    }



    @Override
    public Result run(String query ,TransactionConfig config)
    {
        return this.conn.run(query ,config);
    }



    @Override
    public Result run(String query ,Map<String ,Object> parameters ,TransactionConfig config)
    {
        return this.conn.run(query ,parameters ,config);
    }



    @Override
    public Result run(Query query ,TransactionConfig config)
    {
        return this.conn.run(query ,config);
    }



    @Override
    public Bookmark lastBookmark()
    {
        return this.conn.lastBookmark();
    }



    @SuppressWarnings("deprecation")
    @Override
    public void reset()
    {
        this.conn.reset();
    }
    
}
