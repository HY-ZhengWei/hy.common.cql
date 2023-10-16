package org.hy.common.xcql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hy.common.Execute;
import org.hy.common.Help;
import org.hy.common.XJavaID;





/**
 * XCQL的触发器。
 * 
 *   类似于数据库的After触发器，但也有区别：
 *     1. 对Insert、Update、Delete语句有效外，还对 SELECT语句、存储过程、函数及其它DDL、DML、DCL、TCL均均生效，均可触发。
 *     2. 一个XCQL可以触发多个触发器，并且可以递归触发（即触发器的触发器）。
 *     3. XCQL触发源的执行入参，会传递给所有XCQL触发器，并作为其执行入参。
 *     4. 因为每个XCQL触发器均一个XCQL对象，每个XCQL对象可以有自己的数据库，所以触发源与触发器间、触发器与触发器间均可实现跨数据库的触发器功能。
 *     5. 触发器执行的时长，是不统计在触发源XCQL的执行时长中的。
 *     6. XCQL触发器分为“同步模式”和“异步模式”。
 *        6.1 在同步模式的情况下，所有XCQL触发器依次顺序执行，前一个执行完成，后下一个才执行。
 *        6.2 在异步模式的情况下，每个XCQL触发器均是一个独立的线程，所有XCQL触发器几乎是同时执行的。
 *     7. 触发器执行异常后，是不会回滚先前触发源XCQL的操作的（即每个触发器每个操作都是一个独立的事务）。
 *     8. XCQL触发源执行异常时，可以通过XCQLTrigger.errorCode属性控制XCQL触发器是否执行。
 *        默认XCQLTrigger.errorCode为True，即触发源异常时，XCQL触发器也被触发执行。
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-03
 * @version     v1.0
 */
public class XCQLTrigger implements Comparable<XCQLTrigger> ,XJavaID
{
    /** 执行方式之一：XCQL.execute(...) */
    public static final           int $Execute       = 0;
    
    /** 执行方式之一：XCQL.executeUpdate(...) */
    public static final           int $ExecuteUpdate = 1;
    
    
    
    /**
     * 触发器执行操作的集合
     * 
     * 1. 在同步模式(单线程)下，执行按List顺序有序执行。零下标的元素第一个执行。
     * 2. 在异步模式(多线程)下，线程的发起按List有顺序发起。但不一定是有顺序的执行。
     */
    private List<XCQLTriggerInfo> xcqls;
    
    /** 同步模式。默认为：true，即同步模式 */
    private boolean               syncMode;
    
    /**
     * 异常模式。
     * 默认为：true，主XCQL异常时，触发器也被触发执行。
     * 当为： false时，主XCQL执行成功后，触发器才被触发执行。
     */
    private boolean               errorMode;
    
    /** 可自行定制的XCQL异常处理机制。当触发的XCQL未设置异常处理机制（XCQL.getError()==null）时，才生效 */
    private XCQLError             error;
    
    /** 是否初始化 createBackup() 添加的XCQL。只针对 createBackup() 功能的初始化 */
    private boolean               isInit;
    
    /** XJava池中对象的ID标识 */
    private String                xjavaID;
    
    /** 注释。可用于日志的输出等帮助性的信息 */
    private String                comment;
    
    
    
    public XCQLTrigger()
    {
        this.xcqls     = new ArrayList<XCQLTriggerInfo>();
        this.syncMode  = true;
        this.errorMode = true;
        this.error     = null;
        this.isInit    = false;
    }
    
    
    
    /**
     * 触发执行所有的操作
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-05
     * @version     v1.0
     */
    public void executes()
    {
        if ( this.syncMode )
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                if ( v_XCQLTrigger.getExecuteType() == $ExecuteUpdate )
                {
                    v_XCQLTrigger.getXcql().executeUpdate();
                }
                else
                {
                    v_XCQLTrigger.getXcql().execute();
                }
            }
        }
        else
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                if ( v_XCQLTrigger.getExecuteType() == $ExecuteUpdate )
                {
                    (new Execute(v_XCQLTrigger.getXcql() ,"executeUpdate")).start();
                }
                else
                {
                    (new Execute(v_XCQLTrigger.getXcql() ,"execute")).start();
                }
            }
        }
    }
    
    
    
    /**
     * 触发执行所有的操作
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-05
     * @version     v1.0
     *
     * @param i_Values  主XCQL的入参数
     */
    public void executes(Map<String ,?> i_Values)
    {
        if ( this.syncMode )
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                if ( v_XCQLTrigger.getExecuteType() == $ExecuteUpdate )
                {
                    v_XCQLTrigger.getXcql().executeUpdate(i_Values);
                }
                else
                {
                    v_XCQLTrigger.getXcql().execute(i_Values);
                }
            }
        }
        else
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                if ( v_XCQLTrigger.getExecuteType() == $ExecuteUpdate )
                {
                    (new Execute(v_XCQLTrigger.getXcql() ,"executeUpdate" ,i_Values)).start();
                }
                else
                {
                    (new Execute(v_XCQLTrigger.getXcql() ,"execute" ,i_Values)).start();
                }
            }
        }
    }
    
    
    
    /**
     * 触发执行所有的操作
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-05
     * @version     v1.0
     *
     * @param i_Obj  主XCQL的入参数
     */
    public void executes(Object i_Obj)
    {
        if ( this.syncMode )
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                if ( v_XCQLTrigger.getExecuteType() == $ExecuteUpdate )
                {
                    v_XCQLTrigger.getXcql().executeUpdate(i_Obj);
                }
                else
                {
                    v_XCQLTrigger.getXcql().execute(i_Obj);
                }
            }
        }
        else
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                if ( v_XCQLTrigger.getExecuteType() == $ExecuteUpdate )
                {
                    (new Execute(v_XCQLTrigger.getXcql() ,"executeUpdate" ,i_Obj)).start();
                }
                else
                {
                    (new Execute(v_XCQLTrigger.getXcql() ,"execute" ,i_Obj)).start();
                }
            }
        }
    }
    
    
    
    /**
     * 触发执行所有的操作（针对批量执行）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-05
     * @version     v1.0
     *
     * @param i_Obj  主XCQL的入参数
     */
    public void executeUpdates(List<?> i_ObjList)
    {
        if ( this.syncMode )
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                v_XCQLTrigger.getXcql().executeUpdates(i_ObjList);
            }
        }
        else
        {
            for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
            {
                (new Execute(v_XCQLTrigger.getXcql() ,"executeUpdates" ,i_ObjList)).start();
            }
        }
    }
    
    
    
    /**
     * 创建一个备份(数据冗余)的触发器
     * 
     * 这里只需备份数据库的连接池组，其它属性信息均与主数据库一样（属性赋值在其后的操作中设置 XCQL.initTriggers() ）。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-05
     * @version     v1.0
     *
     * @param i_DSCQL  备份数据库的连接信息
     */
    public void setCreateBackup(DataSourceCQL i_DSCQL)
    {
        XCQL v_Trigger = new XCQL();
        
        v_Trigger.setDataSourceCQL(i_DSCQL);
        v_Trigger.setError(this.error);
        
        this.xcqls.add(new XCQLTriggerInfo(v_Trigger ,$ExecuteUpdate));
        
        this.isInit = true;
    }
    
    
    
    /**
     * 创建一个DML触发器
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-08-13
     * @version     v1.0
     *
     * @param i_XCQL
     */
    public void setCreateUpdate(XCQL i_XCQL)
    {
        if ( i_XCQL.getError() == null )
        {
            i_XCQL.setError(this.error);
        }
        this.xcqls.add(new XCQLTriggerInfo(i_XCQL ,$ExecuteUpdate));
    }
    
    
    
    /**
     * 创建一个触发器
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-05
     * @version     v1.0
     *
     * @param i_XCQL
     */
    public void setCreate(XCQL i_XCQL)
    {
        if ( i_XCQL.getError() == null )
        {
            i_XCQL.setError(this.error);
        }
        this.xcqls.add(new XCQLTriggerInfo(i_XCQL ,$Execute));
    }
    
    
    /**
     * 请求数据库的次数
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-06
     * @version     v1.0
     *
     * @return
     */
    public long getRequestCount()
    {
        long v_Ret = 0;
        
        if ( Help.isNull(this.xcqls) )
        {
            return v_Ret;
        }
        
        for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
        {
            v_Ret += v_XCQLTrigger.getXcql().getRequestCount();
        }
        
        return v_Ret;
    }
    
    
    
    /**
     * 请求成功，并成功返回次数
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-06
     * @version     v1.0
     *
     * @return
     */
    public long getSuccessCount()
    {
        long v_Ret = 0;
        
        if ( Help.isNull(this.xcqls) )
        {
            return v_Ret;
        }
        
        for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
        {
            v_Ret += v_XCQLTrigger.getXcql().getSuccessCount();
        }
        
        return v_Ret;
    }
    
    
    
    /**
     * 请求成功，并成功返回的累计用时时长。
     * 用的是Double，而不是long，因为在批量执行时。为了精度，会出现小数
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-01-06
     * @version     v1.0
     *
     * @return
     */
    public double getSuccessTimeLen()
    {
        double v_Ret = 0;
        
        if ( Help.isNull(this.xcqls) )
        {
            return v_Ret;
        }
        
        for (XCQLTriggerInfo v_XCQLTrigger : this.xcqls)
        {
            v_Ret += v_XCQLTrigger.getXcql().getSuccessTimeLen();
        }
        
        return v_Ret;
    }
    
    
    
    /**
     * 获取：触发器执行操作的集合
     * 
     * 1. 在同步模式(单线程)下，执行按List顺序有序执行。零下标的元素第一个执行。
     * 2. 在异步模式(多线程)下，线程的发起按List有顺序发起。但不一定是有顺序的执行。
     */
    public List<XCQLTriggerInfo> getXcqls()
    {
        return xcqls;
    }


    
    /**
     * 设置：触发器执行操作的集合
     * 
     * 1. 在同步模式(单线程)下，执行按List顺序有序执行。零下标的元素第一个执行。
     * 2. 在异步模式(多线程)下，线程的发起按List有顺序发起。但不一定是有顺序的执行。
     * 
     * @param i_Xcqls
     */
    public void setXcqls(List<XCQLTriggerInfo> i_Xcqls)
    {
        this.xcqls = i_Xcqls;
    }


    
    /**
     * 获取：同步模式。默认为：true，即同步模式
     */
    public boolean isSyncMode()
    {
        return syncMode;
    }


    
    /**
     * 设置：同步模式。默认为：true，即同步模式
     * 
     * @param syncMode
     */
    public void setSyncMode(boolean syncMode)
    {
        this.syncMode = syncMode;
    }


    
    /**
     * 获取：异常模式。
     * 默认为：false，主XCQL执行成功后，触发器才被触发执行
     * 当为：true时， 主XCQL异常时，触发器也被触发执行
     */
    public boolean isErrorMode()
    {
        return errorMode;
    }


    
    /**
     * 设置：异常模式。
     * 默认为：false，主XCQL执行成功后，触发器才被触发执行
     * 当为：true时， 主XCQL异常时，触发器也被触发执行
     * 
     * @param errorMode
     */
    public void setErrorMode(boolean errorMode)
    {
        this.errorMode = errorMode;
    }



    /**
     * 获取：可自行定制的XCQL异常处理机制。当触发的XCQL未设置异常处理机制（XCQL.getError()==null）时，才生效
     */
    public XCQLError getError()
    {
        return error;
    }


    
    /**
     * 设置：可自行定制的XCQL异常处理机制。当触发的XCQL未设置异常处理机制（XCQL.getError()==null）时，才生效
     * 
     * @param error
     */
    public void setError(XCQLError error)
    {
        this.error = error;
    }


    
    /**
     * 获取：是否初始化过所有的XCQL。只针对 createBackup() 功能的初始化
     */
    public boolean isInit()
    {
        return isInit;
    }


    
    /**
     * 设置：是否初始化过所有的XCQL。只针对 createBackup() 功能的初始化
     * 
     * @param isInit
     */
    public void setInit(boolean isInit)
    {
        this.isInit = isInit;
    }
    
    
    
    /**
     * 获取：XJava池中对象的ID标识
     */
    @Override
    public String getXJavaID()
    {
        return xjavaID;
    }


    
    /**
     * 设置：XJava池中对象的ID标识
     * 
     * @param i_XjavaID XJava池中对象的ID标识
     */
    @Override
    public void setXJavaID(String i_XjavaID)
    {
        this.xjavaID = i_XjavaID;
    }


    
    /**
     * 获取：注释。可用于日志的输出等帮助性的信息
     */
    @Override
    public String getComment()
    {
        return comment;
    }


    
    /**
     * 设置：注释。可用于日志的输出等帮助性的信息
     * 
     * @param i_Comment 注释。可用于日志的输出等帮助性的信息
     */
    @Override
    public void setComment(String i_Comment)
    {
        this.comment = i_Comment;
    }
    
    
    
    @Override
    public int compareTo(XCQLTrigger i_XCQLTrigger)
    {
        if ( i_XCQLTrigger == null )
        {
            return 1;
        }
        else if ( this == i_XCQLTrigger )
        {
            return 0;
        }
        else if ( Help.isNull(this.getXJavaID()) )
        {
            return -1;
        }
        else if ( Help.isNull(i_XCQLTrigger.getXJavaID()) )
        {
            return 1;
        }
        else
        {
            return this.getXJavaID().compareTo(i_XCQLTrigger.getXJavaID());
        }
    }
    
    
    
    @Override
    public boolean equals(Object i_Other)
    {
        if ( i_Other == null )
        {
            return false;
        }
        else if ( this == i_Other )
        {
            return true;
        }
        else if ( i_Other instanceof XCQLTrigger )
        {
            XCQLTrigger v_Other = (XCQLTrigger) i_Other;
            
            if ( Help.isNull(this.getXJavaID()) )
            {
                return false;
            }
            else if ( Help.isNull(v_Other.getXJavaID()) )
            {
                return false;
            }
            else
            {
                return this.getXJavaID().equals(v_Other.getXJavaID());
            }
        }
        else
        {
            return false;
        }
    }
}
