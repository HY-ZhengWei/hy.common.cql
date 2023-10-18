package org.hy.common.xcql;

import java.util.List;
import java.util.Map;

import org.hy.common.Date;
import org.hy.common.Help;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;





/**
 * XCQL功能中Create\Set\Delete语句的具体操作与实现。
 * 
 * 核心区别：它与executeUpdate方法的核心区别是：方法返回类型为：XCQLData
 * 独立原因：从XCQL主类中分离的主要原因是：减少XCQL主类的代码量，方便维护。使XCQL主类向外提供统一的操作，本类重点关注实现。
 * 静态原因：用static方法的原因：不想再构建太多的类实例，减少内存负担
 * 接口选择：未使用接口的原因：本类的每个方法的首个入参都有一个XCQL类型，并且都是static方法
 * 
 * @author      ZhengWei(HY)
 * @createDate  2022-05-23
 * @version     v1.0
 *              v2.0  2023-10-18  添加：是否附加触发额外参数的功能
 */
public class XCQLOPInsert
{
    
    /**
     * 占位符CQL的Create\Set\Delete语句的执行。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_XCQL
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,(Object) null);
        long                v_IORowCount    = 0L;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL   = i_XCQL.getContent().getCQL(v_DSCQL);
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes();
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Map<String ,?> i_Values)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,i_Values);
        long                v_IORowCount    = 0L;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesMap(i_Values));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesMap(i_Values));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes(i_Values);
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values              占位符CQL的填充对象。
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Object i_Values)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,i_Values);
        long                v_IORowCount    = 0L;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Values));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Values));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes(i_Values);
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 常规Create\Set\Delete语句的执行。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final String i_CQL)
    {
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,(Object) null);
        long                v_IORowCount    = 0L;

        try
        {
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,i_CQL ,i_XCQL.getDataSourceCQL());
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes();
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 常规Create\Set\Delete语句的执行。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    private static XCQLData executeInsert_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSG)
    {
        Connection v_Conn      = null;
        Result     v_Result    = null;
        long       v_BeginTime = i_XCQL.request().getTime();
        
        try
        {
            if ( !i_DSG.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + i_DSG.getXJavaID() + "] is not valid.");
            }
            
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            v_Conn   = i_XCQL.getConnection(i_DSG);
            v_Result = v_Conn.run(i_CQL);
            i_XCQL.log(i_CQL);
            
            int v_RowCount = v_Result.consume().counters().nodesCreated()
                           + v_Result.consume().counters().nodesDeleted();
            int v_ColCount = v_Result.consume().counters().propertiesSet();
            int v_RelCount = v_Result.consume().counters().relationshipsCreated()
                           + v_Result.consume().counters().relationshipsDeleted();
            
            int v_Count = v_RowCount + v_RelCount;
            // 当并非创建、删除节点和关系时，才取对属性的操作数量
            if ( v_Count <= 0 )
            {
                v_Count = v_ColCount;
            }
            
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,1 ,v_Count);
            
            return new XCQLData(null ,v_RowCount ,v_ColCount ,v_RelCount ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(v_Result ,v_Conn);
        }
    }
    
    
    
    /**
     * 占位符CQL的Create\Set\Delete语句的执行。 -- 无填充值的（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Conn             数据库连接
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,(Object) null);
        long                v_IORowCount    = 0L;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL   = i_XCQL.getContent().getCQL(v_DSCQL);
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,i_Conn);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes();
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的Create\Set\Delete语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn             数据库连接
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Map<String ,?> i_Values ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,i_Values);
        long                v_IORowCount    = 0L;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;
        
        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,i_Conn);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesMap(i_Values));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesMap(i_Values));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes(i_Values);
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的Create\Set\Delete语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Values 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充对象。
     * @param i_Conn             数据库连接
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Object i_Values ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,i_Values);
        long                v_IORowCount    = 0L;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,i_Conn);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Values));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Values));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes(i_Values);
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 常规Create\Set\Delete语句的执行。（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInsert" ,(Object) null);
        long                v_IORowCount    = 0L;

        try
        {
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,i_CQL ,i_Conn);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executes();
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 常规Create\Set\Delete语句的执行。（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return
     */
    private static XCQLData executeInsert_Inner(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        long v_BeginTime = i_XCQL.request().getTime();
        
        try
        {
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            if ( null == i_Conn )
            {
                throw new NullPointerException("Connection is null of XCQL.");
            }
            
            Result v_Result = i_Conn.run(i_CQL);
            int v_RowCount = v_Result.consume().counters().nodesCreated()
                           + v_Result.consume().counters().nodesDeleted();
            int v_ColCount = v_Result.consume().counters().propertiesSet();
            int v_RelCount = v_Result.consume().counters().relationshipsCreated()
                           + v_Result.consume().counters().relationshipsDeleted();
            
            int v_Count    = v_RowCount + v_RelCount;
            // 当并非创建、删除节点和关系时，才取对属性的操作数量
            if ( v_Count <= 0 )
            {
                v_Count = v_ColCount;
            }
            
            i_XCQL.log(i_CQL);
            
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,1 ,v_Count);
            
            return new XCQLData(null ,v_RowCount ,v_ColCount ,v_RelCount ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(null ,null);
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @return                   返回语句影响的记录数。
     */
    public static XCQLData executeInserts(final XCQL i_XCQL ,final List<?> i_ObjList)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInserts" ,(Object) null);
        long                v_IORowCount    = 0L;

        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            XCQLData v_Ret = XCQLOPInsert.executeInserts_Inner(i_XCQL ,i_ObjList ,null);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executeUpdates(i_ObjList);
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Create\Set\Delete语句的执行。
     * 
     *   注意：不支持Delete语句
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @param i_Conn             数据库连接。
     *                           1. 当为空时，内部自动获取一个新的数据库连接。
     *                           2. 当有值时，内部将不关闭数据库连接，而是交给外部调用者来关闭。
     *                           3. 当有值时，内部也不执行"提交"操作（但分批提交this.batchCommit大于0时除外），而是交给外部调用者来执行"提交"。
     *                           4. 当有值时，出现异常时，内部也不执行"回滚"操作，而是交给外部调用者来执行"回滚"。
     * @return                   返回语句影响的记录数。
     */
    public static XCQLData executeInserts(final XCQL i_XCQL ,final List<?> i_ObjList ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeInserts" ,(Object) null);
        long                v_IORowCount    = 0L;

        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            XCQLData v_Ret = XCQLOPInsert.executeInserts_Inner(i_XCQL ,i_ObjList ,i_Conn);
            v_IORowCount = v_Ret.getRowCount();
            return v_Ret;
        }
        catch (NullPointerException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError   = true;
            v_ErrorInfo = Help.NVL(exce.getMessage() ,"E");
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                if ( v_TriggerParams == null )
                {
                    i_XCQL.getTrigger().executeUpdates(i_ObjList);
                }
                else
                {
                    i_XCQL.getTrigger().executes(i_XCQL.executeAfterForTrigger(v_TriggerParams ,v_IORowCount ,v_ErrorInfo));
                }
            }
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @param i_Conn             数据库连接。
     *                           1. 当为空时，内部自动获取一个新的数据库连接。
     *                           2. 当有值时，内部将不关闭数据库连接，而是交给外部调用者来关闭。
     *                           3. 当有值时，内部也不执行"提交"操作（但分批提交this.batchCommit大于0时除外），而是交给外部调用者来执行"提交"。
     *                           4. 当有值时，出现异常时，内部也不执行"回滚"操作，而是交给外部调用者来执行"回滚"。
     * @return                   返回语句影响的记录数。
     */
    private static XCQLData executeInserts_Inner(final XCQL i_XCQL ,final List<?> i_ObjList ,final Connection i_Conn)
    {
        DataSourceCQL v_DSCQL       = null;
        Connection    v_Conn        = null;
        Transaction   v_Transaction = null;
        Result        v_Result      = null;
        int           v_Ret         = 0;
        long          v_BeginTime   = i_XCQL.request().getTime();
        String        v_CQL         = null;
        int           v_RowCount    = 0;
        int           v_ColCount    = 0;
        int           v_RelCount    = 0;
        
        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            if ( !v_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + v_DSCQL.getXJavaID() + "] is not valid.");
            }
            
            if ( Help.isNull(i_ObjList) )
            {
                throw new NullPointerException("Batch execute update List<Object> is null.");
            }
            
            if ( i_Conn == null )
            {
                v_Conn = i_XCQL.getConnection(v_DSCQL);
            }
            else
            {
                v_Conn = i_Conn;
            }
            
            v_Transaction = v_Conn.beginTransaction();
            
            if ( i_XCQL.getBatchCommit() <= 0 )
            {
                for (int i=0; i<i_ObjList.size(); i++)
                {
                    if ( i_ObjList.get(i) != null )
                    {
                        v_CQL       = i_XCQL.getContent().getCQL(i_ObjList.get(i) ,v_DSCQL);
                        v_Result    = v_Transaction.run(v_CQL);
                        v_RowCount += v_Result.consume().counters().nodesCreated()
                                    + v_Result.consume().counters().nodesDeleted();
                        v_ColCount += v_Result.consume().counters().propertiesSet();
                        v_RelCount += v_Result.consume().counters().relationshipsCreated()
                                    + v_Result.consume().counters().relationshipsDeleted();
                        
                        i_XCQL.log(v_CQL);
                    }
                }
                
                if ( i_Conn == null )
                {
                    v_Transaction.commit();  // 它与i_Conn.commit();同作用
                }
            }
            else
            {
                boolean v_IsCommit = true;
                
                for (int i=0 ,v_EC=0; i<i_ObjList.size(); i++)
                {
                    if ( i_ObjList.get(i) != null )
                    {
                        v_CQL       = i_XCQL.getContent().getCQL(i_ObjList.get(i) ,v_DSCQL);
                        v_Result    = v_Transaction.run(v_CQL);
                        v_RowCount += v_Result.consume().counters().nodesCreated()
                                    + v_Result.consume().counters().nodesDeleted();
                        v_ColCount += v_Result.consume().counters().propertiesSet();
                        v_RelCount += v_Result.consume().counters().relationshipsCreated()
                                    + v_Result.consume().counters().relationshipsDeleted();
                    
                        i_XCQL.log(v_CQL);
                        v_EC++;
                        
                        if ( v_EC % i_XCQL.getBatchCommit() == 0 )
                        {
                            v_Transaction.commit();
                            v_IsCommit = true;
                        }
                        else
                        {
                            v_IsCommit = false;
                        }
                    }
                }
                
                if ( !v_IsCommit )
                {
                    v_Transaction.commit();
                }
            }
            
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,i_ObjList.size() ,v_Ret);
            return new XCQLData(null ,v_RowCount ,v_ColCount ,v_RelCount ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            XCQL.erroring(v_CQL ,exce ,i_XCQL);
            
            try
            {
                if ( i_Conn == null && v_Conn != null )
                {
                    v_Transaction.rollback();
                }
            }
            catch (Exception e)
            {
                // Nothing.
            }
            
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            if ( i_Conn == null )
            {
                i_XCQL.closeDB(null ,v_Conn);
            }
            else
            {
                i_XCQL.closeDB(null ,null);
            }
        }
    }
    
    
    
    /**
     * 本类不允许构建
     */
    private XCQLOPInsert()
    {
        
    }
    
}
