package org.hy.common.xcql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hy.common.Date;
import org.hy.common.Help;
import org.hy.common.PartitionMap;
import org.hy.common.Return;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;





/**
 * XCQL功能中Create\Set\Delete语句的具体操作与实现。
 * 
 * 独立原因：从XCQL主类中分离的主要原因是：减少XCQL主类的代码量，方便维护。使XCQL主类向外提供统一的操作，本类重点关注实现。
 * 静态原因：用static方法的原因：不想再构建太多的类实例，减少内存负担
 * 接口选择：未使用接口的原因：本类的每个方法的首个入参都有一个XCQL类型，并且都是static方法
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-05
 * @version     v1.0
 *              v2.0  2023-10-18  添加：是否附加触发额外参数的功能
 */
public class XCQLOPUpdate
{
    
    /**
     * 占位符CQL的Create\Set\Delete语句的执行。 -- 无填充值的
     * 
     * @return  返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,(Object) null);
        int                 v_IORowCount    = 0;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL   = i_XCQL.getContent().getCQL(v_DSCQL);
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            return v_IORowCount;
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
     * @param i_Values  占位符CQL的填充集合。
     * @return          返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL ,final Map<String ,?> i_Values)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,i_Values);
        int                 v_IORowCount    = 0;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL   = i_XCQL.getDataSourceCQL();
            v_CQL     = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            return v_IORowCount;
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
     * 1. 按对象 i_Values 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Values  占位符CQL的填充对象。
     * @return          返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL ,final Object i_Values)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,i_Values);
        int                 v_IORowCount    = 0;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL   = i_XCQL.getDataSourceCQL();
            v_CQL     = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            return v_IORowCount;
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
     * @param i_CQL  常规CQL语句
     * @return       返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL ,final String i_CQL)
    {
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,(Object) null);
        int                 v_IORowCount    = 0;

        try
        {
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,i_CQL ,i_XCQL.getDataSourceCQL());
            return v_IORowCount;
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
     * @param i_CQL  常规CQL语句
     * @return       返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    private static int executeUpdate_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSG)
    {
        Connection v_Conn      = null;
        Result     v_Result    = null;
        long       v_BeginTime = i_XCQL.request().getTime();
        
        try
        {
            if ( !i_DSG.isValid() )
            {
                throw new RuntimeException("DataSourceCQL is not valid.");
            }
            
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            v_Conn   = i_XCQL.getConnection(i_DSG);
            v_Result = v_Conn.run(i_CQL);
            
            int v_Count = v_Result.consume().counters().nodesCreated()
                        + v_Result.consume().counters().nodesDeleted()
                        + v_Result.consume().counters().relationshipsCreated()
                        + v_Result.consume().counters().relationshipsDeleted();
            // 当并非创建、删除节点和关系时，才取对属性的操作数量
            if ( v_Count <= 0 )
            {
                v_Count = v_Result.consume().counters().propertiesSet();
            }
            
            i_XCQL.log(i_CQL);
            
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,v_Count);
            
            return v_Count;
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
     * @param i_Conn  数据库连接
     * @return        返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,(Object) null);
        int                 v_IORowCount    = 0;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL   = i_XCQL.getContent().getCQL(v_DSCQL);
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,v_CQL ,i_Conn);
            return v_IORowCount;
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
     * @param i_Values 占位符CQL的填充集合。
     * @param i_Conn   数据库连接
     * @return         返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL ,final Map<String ,?> i_Values ,Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,i_Values);
        int                 v_IORowCount    = 0;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL   = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,v_CQL ,i_Conn);
            return v_IORowCount;
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
     * 占位符CQL的Create\Set\Delete语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Values   占位符CQL的填充对象。
     * @param i_Conn     数据库连接
     * @return           返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL ,final Object i_Values ,Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,i_Values);
        int                 v_IORowCount    = 0;
        DataSourceCQL       v_DSCQL         = null;
        String              v_CQL           = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL   = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,v_CQL ,i_Conn);
            return v_IORowCount;
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
     * @param i_CQL   常规CQL语句
     * @param i_Conn  数据库连接
     * @return        返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdate(final XCQL i_XCQL ,final String i_CQL ,Connection i_Conn)
    {
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,(Object) null);
        int                 v_IORowCount    = 0;

        try
        {
            v_IORowCount = XCQLOPUpdate.executeUpdate_Inner(i_XCQL ,i_CQL ,i_Conn);
            return v_IORowCount;
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
     * @param i_XCQL  XCQL对象
     * @param i_CQL   常规CQL语句
     * @param i_Conn  数据库连接
     * @return        返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    private static int executeUpdate_Inner(final XCQL i_XCQL ,final String i_CQL ,Connection i_Conn)
    {
        Result v_Result    = null;
        long   v_BeginTime = i_XCQL.request().getTime();
        
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
            
            v_Result = i_Conn.run(i_CQL);
            
            int v_Count = v_Result.consume().counters().nodesCreated()
                        + v_Result.consume().counters().nodesDeleted()
                        + v_Result.consume().counters().relationshipsCreated()
                        + v_Result.consume().counters().relationshipsDeleted();
            // 当并非创建、删除节点和关系时，才取对属性的操作数量
            if ( v_Count <= 0 )
            {
                v_Count = v_Result.consume().counters().propertiesSet();
            }
            
            i_XCQL.log(i_CQL);
            
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,v_Count);
            
            return v_Count;
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(v_Result ,null);
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @param i_ObjList  占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @return           返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdates(final XCQL i_XCQL ,final List<?> i_ObjList)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,(Object) null);
        int                 v_IORowCount    = 0;

        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            v_IORowCount = XCQLOPUpdate.executeUpdates_Inner(i_XCQL ,i_ObjList ,null);
            return v_IORowCount;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * @param i_ObjList  占位符CQL的填充对象的集合。
     *                   1. 集合元素可以是Object
     *                   2. 集合元素可以是Map<String ,?>
     *                   3. 更可以是上面两者的混合元素组成的集合
     * @param i_Conn     数据库连接。
     *                   1. 当为空时，内部自动获取一个新的数据库连接。
     *                   2. 当有值时，内部将不关闭数据库连接，而是交给外部调用者来关闭。
     *                   3. 当有值时，内部也不执行"提交"操作（但分批提交i_XCQL.getBatchCommit()大于0时除外），而是交给外部调用者来执行"提交"。
     *                   4. 当有值时，出现异常时，内部也不执行"回滚"操作，而是交给外部调用者来执行"回滚"。
     * @return           返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdates(final XCQL i_XCQL ,final List<?> i_ObjList ,Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean             v_IsError       = false;
        String              v_ErrorInfo     = null;
        Map<String ,Object> v_TriggerParams = i_XCQL.executeBeforeForTrigger("executeUpdate" ,(Object) null);
        int                 v_IORowCount    = 0;

        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            v_IORowCount = XCQLOPUpdate.executeUpdates_Inner(i_XCQL ,i_ObjList ,i_Conn);
            return v_IORowCount;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * @param i_ObjList  占位符CQL的填充对象的集合。
     *                   1. 集合元素可以是Object
     *                   2. 集合元素可以是Map<String ,?>
     *                   3. 更可以是上面两者的混合元素组成的集合
     * @param i_Conn     数据库连接。
     *                   1. 当为空时，内部自动获取一个新的数据库连接。
     *                   2. 当有值时，内部将不关闭数据库连接，而是交给外部调用者来关闭。
     *                   3. 当有值时，内部也不执行"提交"操作（但分批提交i_XCQL.getBatchCommit()大于0时除外），而是交给外部调用者来执行"提交"。
     *                   4. 当有值时，出现异常时，内部也不执行"回滚"操作，而是交给外部调用者来执行"回滚"。
     * @return           返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    private static int executeUpdates_Inner(final XCQL i_XCQL ,final List<?> i_ObjList ,final Connection i_Conn)
    {
        DataSourceCQL v_DSCQL       = null;
        Connection    v_Conn        = null;
        Transaction   v_Transaction = null;
        Result        v_Result      = null;
        int           v_Ret         = 0;
        long          v_BeginTime   = i_XCQL.request().getTime();
        String        v_CQL         = null;
        int           v_CQLCount    = 0;
        
        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            if ( !v_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL is not valid.");
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
                        v_CQL      = i_XCQL.getContent().getCQL(i_ObjList.get(i) ,v_DSCQL);
                        v_Result   = v_Transaction.run(v_CQL);
                        v_CQLCount = v_Result.consume().counters().nodesCreated()
                                   + v_Result.consume().counters().nodesDeleted()
                                   + v_Result.consume().counters().relationshipsCreated()
                                   + v_Result.consume().counters().relationshipsDeleted();
                        
                        // 当并非创建、删除节点和关系时，才取对属性的操作数量
                        if ( v_CQLCount <= 0 )
                        {
                            v_CQLCount = v_Result.consume().counters().propertiesSet();
                        }
                        
                        if ( v_CQLCount >= 1 )
                        {
                            v_Ret += v_CQLCount;
                        }
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
                        v_CQL      = i_XCQL.getContent().getCQL(i_ObjList.get(i) ,v_DSCQL);
                        v_Result   = v_Transaction.run(v_CQL);
                        v_CQLCount = v_Result.consume().counters().nodesCreated()
                                   + v_Result.consume().counters().nodesDeleted()
                                   + v_Result.consume().counters().relationshipsCreated()
                                   + v_Result.consume().counters().relationshipsDeleted();
                     
                        // 当并非创建、删除节点和关系时，才取对属性的操作数量
                        if ( v_CQLCount <= 0 )
                        {
                            v_CQLCount = v_Result.consume().counters().propertiesSet();
                        }
                        
                        if ( v_CQLCount >= 1 )
                        {
                            v_Ret += v_CQLCount;
                        }
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
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,i_ObjList.size() ,v_Ret);
            return v_Ret;
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
     * 批量执行：占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注: 1. 支持多种不同CQL语句的执行
     *     2. 支持不同类型的多个不同数据库的操作
     *     3. 如果要有顺序的执行，请java.util.LinkedHashMap
     * 
     * 重点注意：2014-12-04
     *         建议入参使用 TablePartition。为什么呢？
     *         原因是，Hashtable.put() 同样的key多次，只保存一份value。
     *         而 TablePartition.putRows() 会将同样key的多份不同的value整合在一起。
     *         特别适应于同一份Create\Set\Delete语句的CQL，执行多批数据的插入的情况
     * 
     * @param i_XCQLs   XCQL及占位符CQL的填充对象的集合。
     *                  1. List<?>集合元素可以是Object
     *                  2. List<?>集合元素可以是Map<String ,?>
     *                  3. List<?>更可以是上面两者的混合元素组成的集合
     * @return          返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    @SuppressWarnings({"unchecked" ,"rawtypes"})
    public static int executeUpdates(final PartitionMap<XCQL ,?> i_XCQLs)
    {
        return XCQLOPUpdate.executeUpdates((Map)i_XCQLs ,0);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注: 1. 支持多种不同CQL语句的执行
     *     2. 支持不同类型的多个不同数据库的操作
     *     3. 如果要有顺序的执行，请java.util.LinkedHashMap
     * 
     * 重点注意：2014-12-04
     *         建议入参使用 TablePartition。为什么呢？
     *         原因是，Hashtable.put() 同样的key多次，只保存一份value。
     *         而 TablePartition.putRows() 会将同样key的多份不同的value整合在一起。
     *         特别适应于同一份Create\Set\Delete语句的CQL，执行多批数据的插入的情况
     * 
     * @param i_XCQLs  XCQL及占位符CQL的填充对象的集合。
     *                 1. List<?>集合元素可以是Object
     *                 2. List<?>集合元素可以是Map<String ,?>
     *                 3. List<?>更可以是上面两者的混合元素组成的集合
     * @return         返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static int executeUpdates(final Map<XCQL ,List<?>> i_XCQLs)
    {
        return XCQLOPUpdate.executeUpdates(i_XCQLs ,0);
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Create\Set\Delete语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注: 1. 支持多种不同CQL语句的执行
     *     2. 支持不同类型的多个不同数据库的操作
     *     3. 如果要有顺序的执行，请java.util.LinkedHashMap
     *
     * 重点注意：2014-12-04
     *         建议入参使用 TablePartition<XCQL ,?>，（注意不是 TablePartition<XCQL ,List<?>>）
     *         为什么呢？
     *         原因是，Hashtable.put() 同样的key多次，只保存一份value。
     *         而 TablePartition.putRows() 会将同样key的多份不同的value整合在一起。
     *         特别适应于同一份Create语句的CQL，执行多批数据的插入的情况
     * 
     * @param i_XCQLs        XCQL及占位符CQL的填充对象的集合。
     *                       1. List<?>集合元素可以是Object
     *                       2. List<?>集合元素可以是Map<String ,?>
     *                       3. List<?>更可以是上面两者的混合元素组成的集合
     * @param i_BatchCommit  批量执行 Create\Set\Delete 时，达到提交的提交点
     * @return               返回语句影响的数量（创建、删除节点和关系时，返回影响的节点数量；非节点和关系操作时，才取对属性的影响数量）
     */
    public static <R> int executeUpdates(final Map<XCQL ,List<?>> i_XCQLs ,final int i_BatchCommit)
    {
        Map<XCQL ,DataSourceCQL> v_DSCQLMap       = new HashMap<XCQL ,DataSourceCQL>();
        DataSourceCQL            v_DSCQL          = null;
        List<Connection>         v_Conns          = new ArrayList<Connection>();
        XCQL                     v_XCQL           = null;
        XCQL                     v_XCQLError      = null;
        Object                   v_ParamObj       = null;
        int                      v_Ret            = 0;
        long                     v_TimeLenSum     = 0;                               // 每段CQL用时时长的累计值。此值一般情况下小于 v_TimeLenTotal
        long                     v_TimeLenTotal   = 0;                               // 总体用时时长
        long                     v_BeginTimeTotal = Date.getNowTime().getTime();     // 总体开始时间
        long                     v_BeginTime      = 0;
        Date                     v_EndTime        = null;
        List<Return<XCQL>>       v_Totals         = null;
        Return<XCQL>             v_TotalCache     = null;
        
        try
        {
            if ( Help.isNull(i_XCQLs) )
            {
                throw new NullPointerException("XCQLs is null.");
            }
            
            for (XCQL v_XCQLTemp : i_XCQLs.keySet())
            {
                if ( v_XCQLTemp.getContent() == null )
                {
                    throw new NullPointerException("Content is null of XCQL.");
                }
                
                if ( Help.isNull(i_XCQLs.get(v_XCQLTemp)) )
                {
                    throw new NullPointerException("Batch execute update List<Object> is null.");
                }
                
                v_DSCQL = v_XCQLTemp.getDataSourceCQL();
                if ( !v_DSCQL.isValid() )
                {
                    throw new RuntimeException("DataSourceCQL is not valid.");
                }
                v_DSCQLMap.put(v_XCQLTemp ,v_DSCQL);
            }
            
            v_Totals = new ArrayList<Return<XCQL>>(i_XCQLs.size());
            v_XCQL   = i_XCQLs.keySet().iterator().next();
            
            if ( i_BatchCommit <= 0 )
            {
                for (XCQL v_XCQLTemp : i_XCQLs.keySet())
                {
                    Connection v_Conn = v_XCQL.getConnection(v_DSCQL);
                    v_Conns.add(v_Conn);
                    
                    v_XCQLError = v_XCQLTemp;
                    List<?> v_ObjList = i_XCQLs.get(v_XCQLTemp);
                    
                    for (int i=0; i<v_ObjList.size(); i++)
                    {
                        v_ParamObj = v_ObjList.get(i);
                        
                        if ( v_ParamObj != null )
                        {
                            v_BeginTime = Date.getNowTime().getTime();
                            v_Ret += v_XCQLTemp.executeUpdate(v_ParamObj ,v_Conn);
                            
                            v_TotalCache = new Return<XCQL>();
                            v_TotalCache.paramInt((int)(Date.getNowTime().getTime() - v_BeginTime));
                            v_Totals.add(v_TotalCache.paramObj(v_XCQLTemp));
                            
                            v_TimeLenSum += v_TotalCache.paramInt;
                        }
                    }
                }
                
                XCQL.commits(1 ,v_Conns);
            }
            else
            {
                boolean v_IsCommit = true;
                
                for (XCQL v_XCQLTemp : i_XCQLs.keySet())
                {
                    Connection  v_Conn        = v_XCQL.getConnection(v_DSCQL);
                    Transaction v_Transaction = v_Conn.beginTransaction();
                    v_Conns.add(v_Conn);
                    
                    v_XCQLError = v_XCQLTemp;
                    List<?> v_ObjList = i_XCQLs.get(v_XCQLTemp);
                    
                    for (int i=0; i<v_ObjList.size(); i++)
                    {
                        v_ParamObj = v_ObjList.get(i);
                        
                        if ( v_ParamObj != null )
                        {
                            v_BeginTime = Date.getNowTime().getTime();
                            v_Ret += v_XCQLTemp.executeUpdate(v_ParamObj ,v_Conn);
                            
                            v_TotalCache = new Return<XCQL>();
                            v_TotalCache.paramInt((int)(Date.getNowTime().getTime() - v_BeginTime));
                            v_Totals.add(v_TotalCache.paramObj(v_XCQLTemp));
                            
                            v_TimeLenSum += v_TotalCache.paramInt;
                            
                            if ( i_BatchCommit > 0 && v_Totals.size() % i_BatchCommit == 0 )
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
                        v_IsCommit = true;
                    }
                }
            }
            
            // 计算出总用时与每段CQL累计用时之差，后平摊到每段CQL的用时时长上。
            v_EndTime      = new Date();
            v_TimeLenTotal = v_EndTime.getTime() - v_BeginTimeTotal;
            double v_Per   = Help.division(v_TimeLenTotal - v_TimeLenSum ,v_Totals.size());
            
            // 每个XCQL已自行统计了时间。但当操作数据库成功后，再将平摊的耗时记录在每个XCQL上
            for (Return<XCQL> v_Total : v_Totals)
            {
                v_Total.paramObj.success(v_EndTime ,v_Per ,1 ,0);
            }
            
            return v_Ret;
        }
        catch (Exception exce)
        {
            String v_CQLError = "";
            if ( v_XCQLError != null && v_XCQLError.getContent() != null )
            {
                v_CQLError = v_XCQLError.getContent().getCQL(v_ParamObj ,v_XCQLError.getDataSourceCQL());
            }
            XCQL.erroring(v_CQLError ,exce ,v_XCQLError);
            
            if ( !Help.isNull(v_Conns) )
            {
                XCQL.rollbacks(v_Conns);
            }
            
            // 计算出总用时与每段CQL累计用时之差，后平摊到每段CQL的用时时长上。
            v_EndTime      = new Date();
            v_TimeLenTotal = v_EndTime.getTime() - v_BeginTimeTotal;
            double v_Per   = Help.division(v_TimeLenTotal - v_TimeLenSum ,v_Totals.size());
            
            // 每个XCQL已自行统计了时间。但当操作数据库成功后，再将平摊的耗时记录在每个XCQL上
            for (int i=0; i<v_Totals.size() ; i++)
            {
                Return<XCQL> v_Total = v_Totals.get(i);
                v_Total.paramObj.success(v_EndTime ,v_Per ,1 ,0);
            }
            
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            XCQL.closeDB(v_Conns);
        }
    }
    
    
    
    /**
     * 本类不允许构建
     */
    private XCQLOPUpdate()
    {
        
    }
    
}
