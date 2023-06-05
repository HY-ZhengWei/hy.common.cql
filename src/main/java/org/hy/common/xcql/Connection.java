package org.hy.common.xcql;

import java.util.Map;

import org.hy.common.xml.log.Logger;
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
 * 限制：
 *   同一连接，同时仅能开启一个事物
 *
 * @author      ZhengWei(HY)
 * @createDate  2017-07-13
 * @version     v1.0
 */
public class Connection implements Session
{
    private static final Logger $Logger = new Logger(Connection.class ,true);
    
    
    
    /** 所属的数据库连接信息 */
    private DataSourceCQL       dataSourceCQL;
    
    /** 第三方的连接对象实例 */
    private Session             conn;
    
    /** 第三方的连接开启的事务 */
    private Transaction         transaction;
    
    
    
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
    public synchronized void close()
    {
        try
        {
            if ( this.transaction != null )
            {
                if ( this.transaction.isOpen() )
                {
                    this.transaction.close();
                }
            }
        }
        catch (Exception exce)
        {
            $Logger.error(exce);
        }
        finally
        {
            this.transaction = null;
        }
        
        try
        {
            this.conn.close();
        }
        catch (Exception exce)
        {
            $Logger.error(exce);
        }
        
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
    public synchronized Transaction beginTransaction()
    {
        if ( this.transaction != null )
        {
            if ( !this.transaction.isOpen() )
            {
                this.transaction = this.conn.beginTransaction();
            }
        }
        else
        {
            this.transaction = this.conn.beginTransaction();
        }
        return this.transaction;
    }



    @Override
    public synchronized Transaction beginTransaction(TransactionConfig config)
    {
        if ( this.transaction != null )
        {
            if ( !this.transaction.isOpen() )
            {
                this.transaction = this.conn.beginTransaction(config);
            }
        }
        else
        {
            this.transaction = this.conn.beginTransaction(config);
        }
        return this.transaction;
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
