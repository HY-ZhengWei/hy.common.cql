package org.hy.common.xcql;

import java.io.Serializable;

import org.hy.common.Date;
import org.hy.common.xcql.plugins.XCQLFilter;





/**
 * XCQL 执行日志
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public class XCQLLog implements Serializable
{
    
    private static final long serialVersionUID = -6633622104082741065L;

    /** XCQL 的唯一标识ID */
    private String oid;
    
    /** 执行CQL语句 */
    private String cql;
    
    /** 执行时间。一般执行完成时的时间，或出现异常时的时间 */
    private String time;
    
    /** 执行异常信息 */
    private String e;
    
    
    
    public XCQLLog(String i_CQL)
    {
        this.time  = Date.getNowTime().getFullMilli();
        this.cql   = i_CQL;
        this.e     = "";
        
        this.logXCQL();
    }
    
    
    
    public XCQLLog(String i_CQL ,Exception i_Exce ,String i_XCQLObjectID)
    {
        this.time = Date.getNowTime().getFullMilli();
        this.cql  = i_CQL;
        this.oid  = i_XCQLObjectID;
        
        if ( i_Exce != null )
        {
            this.e = i_Exce.getMessage();
        }
        
        this.logXCQL();
    }
    
    
    
    private void logXCQL()
    {
        XCQLFilter.logXCQL(Thread.currentThread().getId() ,this.cql);
    }
    
    
    
    /**
     * 获取：执行CQL语句
     */
    public String getCql()
    {
        return cql;
    }

    
    /**
     * 设置：执行CQL语句
     * 
     * @param cql
     */
    public void setCql(String cql)
    {
        this.cql = cql;
    }

    
    /**
     * 获取：执行时间
     */
    public String getTime()
    {
        return time;
    }

    
    /**
     * 设置：执行时间
     * 
     * @param time
     */
    public void setTime(String time)
    {
        this.time = time;
    }

    
    /**
     * 获取：执行异常信息
     */
    public String getE()
    {
        return e;
    }

    
    /**
     * 设置：执行异常信息
     * 
     * @param error
     */
    public void setE(String e)
    {
        this.e = e;
    }

    
    /**
     * 获取：XCQL 的唯一标识ID
     */
    public String getOid()
    {
        return oid;
    }

    
    /**
     * 设置：XCQL 的唯一标识ID
     * 
     * @param oid
     */
    public void setOid(String oid)
    {
        this.oid = oid;
    }
    
}
