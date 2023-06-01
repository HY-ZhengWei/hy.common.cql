package org.hy.common.xcql;

import java.io.Serializable;

import org.hy.common.Date;
import org.hy.common.Help;
import org.hy.common.StringHelp;
import org.hy.common.XJavaID;
import org.hy.common.xml.log.Logger;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;





/**
 * 数据库连接信息
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-05-31
 * @version     v1.0
 */
public class DataSourceCQL implements Comparable<DataSourceCQL> ,XJavaID ,Serializable
{
    private static final long   serialVersionUID = -4823712316609404913L;

    private static final Logger $Logger          = new Logger(DataSourceCQL.class ,true);
    
    
    
    /** 唯一标示，主用于对比等操作 */
    private String             uuid;
    
    /** XJava池中对象的ID标识 */
    private String             xjavaID;
    
    /** 注释。可用于日志的输出等帮助性的信息 */
    private String             comment;
    
    /** 连接URL */
    private String             url;
    
    /** 连接用户名称 */
    private String             username;
    
    /** 连接用户密码 */
    private String             password;
    
    /** 连接的数据库实例名称 */
    private String             database;
    
    /** 连接驱动 */
    private Driver             driver;
    
    /** 连接会话配置，如配置连接哪个数据库实例 */
    private SessionConfig      config;
    
    /** 是否出现异常。非连接断连异常也会为true */
    private boolean            isException;
    
    /** 最后一次正常连接的时间 */
    private Date               connLastTime;
    
    /** 活动连接数量（不包括连接池中预先初始化的连接数量） */
    private long               connActiveCount;
    
    /** 连接使用峰值（不包括连接池中预先初始化的连接数量） */
    private long               connMaxUseCount;
    
    
    
    public DataSourceCQL()
    {
        this.uuid            = StringHelp.getUUID();
        this.isException     = false;
        this.connLastTime    = null;
        this.connActiveCount = 0;
        this.connMaxUseCount = 0;
    }
    
    
    
    /**
     * 初始化连接
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-05-31
     * @version     v1.0
     *
     */
    private void initConnection()
    {
        if ( Help.isNull(this.url) )
        {
            this.isException = true;
            NullPointerException v_Error = new NullPointerException(this.getXJavaID() + "：CQL url is null.");
            $Logger.error(v_Error);
            throw v_Error;
        }
        else if ( Help.isNull(this.username) )
        {
            this.isException = true;
            NullPointerException v_Error = new NullPointerException(this.getXJavaID() + "：CQL username is null.");
            $Logger.error(v_Error);
            throw v_Error;
        }
        else if ( Help.isNull(this.password) )
        {
            this.isException = true;
            NullPointerException v_Error = new NullPointerException(this.getXJavaID() + "：CQL password is null.");
            $Logger.error(v_Error);
            throw v_Error;
        }
        
        try
        {
            if ( Help.isNull(this.database) )
            {
                this.config = SessionConfig.defaultConfig();
            }
            else
            {
                this.config = SessionConfig.forDatabase(this.database);
            }
            
            this.driver = GraphDatabase.driver(this.url ,AuthTokens.basic(this.username, this.password));
        }
        catch (Exception exce)
        {
            this.isException = true;
            $Logger.error(exce);
            throw exce;
        }
    }
    
    
    
    /**
     * 获取数据库连接。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-05-31
     * @version     v1.0
     *
     * @return
     */
    public synchronized Session getConnection()
    {
        if ( this.config == null )
        {
            this.initConnection();
        }
        
        Session v_Session = null;
        
        try
        {
            v_Session = this.driver.session(this.config);
            
            if ( v_Session != null )
            {
                this.connActiveCount++;
                if ( this.connActiveCount > this.connMaxUseCount )
                {
                    this.connMaxUseCount = this.connActiveCount;
                }
                this.isException  = false;
                this.connLastTime = new Date();
                
                return new Connection(v_Session ,this);
            }
        }
        catch (Exception exce)
        {
            this.isException = true;
            $Logger.error(exce);
            throw exce;
        }
        
        return null;
    }
    
    
    
    /**
     * 获取：最后一次正常连接的时间
     */
    public Date getConnLastTime()
    {
        return connLastTime;
    }
    
    
    
    /**
     * 获取：活动连接数量（不包括连接池中预先初始化的连接数量）
     */
    public long getConnActiveCount()
    {
        return this.connActiveCount;
    }
    
    
    /**
     * 连接关闭时触发
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-07-13
     * @version     v1.0
     *
     */
    protected synchronized void connClosed()
    {
        this.connActiveCount--;
    }

    
    /**
     * 获取：连接使用峰值（不包括连接池中预先初始化的连接数量）
     */
    public long getConnMaxUseCount()
    {
        return connMaxUseCount;
    }
    
    
    
    /**
     * 获取：是否出现异常。非连接断连异常也会为true
     */
    public boolean isException()
    {
        return isException;
    }

    
    
    /**
     * 设置：是否出现异常。非连接断连异常也会为true
     * 
     * @param isException
     */
    public void setException(boolean isException)
    {
        this.isException = isException;
    }
    
    
    
    /**
     * 获取：连接URL
     */
    public String getUrl()
    {
        return url;
    }


    
    /**
     * 设置：连接URL
     * 
     * @param i_Url 连接URL
     */
    public void setUrl(String i_Url)
    {
        this.url = i_Url;
    }


    
    /**
     * 获取：连接用户名称
     */
    public String getUsername()
    {
        return username;
    }


    
    /**
     * 设置：连接用户名称
     * 
     * @param i_Username 连接用户名称
     */
    public void setUsername(String i_Username)
    {
        this.username = i_Username;
    }


    
    /**
     * 获取：连接用户密码
     */
    public String getPassword()
    {
        return password;
    }


    
    /**
     * 设置：连接用户密码
     * 
     * @param i_Password 连接用户密码
     */
    public void setPassword(String i_Password)
    {
        this.password = i_Password;
    }


    
    /**
     * 获取：连接的数据库实例名称
     */
    public String getDatabase()
    {
        return database;
    }


    
    /**
     * 设置：连接的数据库实例名称
     * 
     * @param i_Database 连接的数据库实例名称
     */
    public void setDatabase(String i_Database)
    {
        this.database = i_Database;
    }



    private String getObjectID()
    {
        return this.uuid;
    }
    
    

    /**
     * 设置XJava池中对象的ID标识。此方法不用用户调用设置值，是自动的。
     * 
     * @param i_XJavaID
     */
    @Override
    public void setXJavaID(String i_XJavaID)
    {
        this.xjavaID = i_XJavaID;
    }
    
    
    
    /**
     * 获取XJava池中对象的ID标识。
     * 
     * @return
     */
    @Override
    public String getXJavaID()
    {
        return this.xjavaID;
    }



    @Override
    public int hashCode()
    {
        return this.getObjectID().hashCode();
    }
    
    
    
    /**
     * 注释。可用于日志的输出等帮助性的信息
     * 
     * @param i_Comment
     */
    @Override
    public void setComment(String i_Comment)
    {
        this.comment = i_Comment;
    }
    
    
    
    /**
     * 注释。可用于日志的输出等帮助性的信息
     *
     * @return
     */
    @Override
    public String getComment()
    {
        return this.comment;
    }
    
    
    
    @Override
    public boolean equals(Object i_Other)
    {
        if ( null == i_Other )
        {
            return false;
        }
        else if ( this == i_Other )
        {
            return true;
        }
        else if ( i_Other instanceof DataSourceCQL )
        {
            return this.getObjectID().equals(((DataSourceCQL)i_Other).getObjectID());
        }
        else
        {
            return false;
        }
    }
    
    
    
    @Override
    public int compareTo(DataSourceCQL i_Other)
    {
        if ( null == i_Other )
        {
            return 1;
        }
        else if ( this == i_Other )
        {
            return 0;
        }
        else
        {
            return this.getObjectID().compareTo(i_Other.getObjectID());
        }
    }
    
}
