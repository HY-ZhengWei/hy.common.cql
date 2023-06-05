package org.hy.common.xcql;

import java.util.Map;

import org.hy.common.Date;
import org.hy.common.Help;





/**
 * XCQL功能中DDL语句的具体操作与实现。
 * 
 * 独立原因：从XCQL主类中分离的主要原因是：减少XCQL主类的代码量，方便维护。使XCQL主类向外提供统一的操作，本类重点关注实现。
 * 静态原因：用static方法的原因：不想再构建太多的类实例，减少内存负担
 * 接口选择：未使用接口的原因：本类的每个方法的首个入参都有一个XCQL类型，并且都是static方法
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-05
 * @version     v1.0
 */
public class XCQLOPDDL
{
    
    /**
     * 占位符CQL的执行。-- 无填充值的
     * 
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,v_DSCQL);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert、Update语法的写入操作。
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL ,final Map<String ,?> i_Values)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            boolean v_Ret = XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            return v_Ret;
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesMap(i_Values));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executes(i_Values);
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert、Update语法的写入操作。
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL ,final Object i_Obj)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;
        
        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            boolean v_Ret = XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            return v_Ret;
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Obj));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Obj));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                i_XCQL.getTrigger().executes(i_Obj);
            }
        }
    }
    
    
    
    /**
     * 常规CQL的执行。
     * 
     * @param i_CQL              常规CQL语句
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL ,final String i_CQL)
    {
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;
        
        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,v_DSCQL);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 常规CQL的执行。
     * 
     * @param i_CQL              常规CQL语句
     * @return                   是否执行成功。
     */
    private static boolean execute_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSCQL)
    {
        Connection v_Conn      = null;
        long       v_BeginTime = i_XCQL.request().getTime();
        String     v_CQL       = i_CQL;
        
        try
        {
            if ( !i_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + i_DSCQL.getXJavaID() + "] is not valid.");
            }
            
            if ( Help.isNull(v_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            v_Conn = i_XCQL.getConnection(i_DSCQL);
            
            if ( i_XCQL.isAllowExecutesSplit() )
            {
                String [] v_CQLs = v_CQL.split(XCQL.$Executes_Split);
                for (int i=0; i<v_CQLs.length; i++)
                {
                    v_CQL = v_CQLs[i].trim();
                    v_Conn.run(v_CQL);
                    i_XCQL.log(v_CQL);
                }
            }
            else
            {
                v_Conn.run(v_CQL);
                i_XCQL.log(v_CQL);
            }
            
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,1L);
            
            return true;
        }
        catch (Exception exce)
        {
            XCQL.erroring(v_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(null ,v_Conn);
        }
    }
    
    
    
    /**
     * 占位符CQL的执行。-- 无填充值的（内部不再关闭数据库连接）
     * 
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,i_Conn);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL ,final Map<String ,?> i_Values ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            return XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,i_Conn);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesMap(i_Values));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executes(i_Values);
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL ,final Object i_Obj ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            return XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,i_Conn);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Obj));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL).setValuesObject(i_Obj));
            }
            throw exce;
        }
        finally
        {
            if ( i_XCQL.isTriggers(v_IsError) )
            {
                i_XCQL.getTrigger().executes(i_Obj);
            }
        }
    }
    
    
    
    /**
     * 常规CQL的执行。（内部不再关闭数据库连接）
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    public static boolean execute(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL   = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPDDL.execute_Inner(i_XCQL ,v_CQL ,i_Conn);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(v_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 常规CQL的执行。（内部不再关闭数据库连接）
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return                   是否执行成功。
     */
    private static boolean execute_Inner(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        long      v_BeginTime = i_XCQL.request().getTime();
        String    v_CQL       = i_CQL;
        
        try
        {
            if ( Help.isNull(v_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            if ( null == i_Conn )
            {
                throw new NullPointerException("Connection is null of XCQL.");
            }
            
            if ( i_XCQL.isAllowExecutesSplit() )
            {
                String [] v_CQLs = v_CQL.split(XCQL.$Executes_Split);
                for (int i=0; i<v_CQLs.length; i++)
                {
                    v_CQL = v_CQLs[i].trim();
                    i_Conn.run(v_CQL);
                    i_XCQL.log(i_CQL);
                }
            }
            else
            {
                i_Conn.run(v_CQL);
                i_XCQL.log(v_CQL);
            }
            
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,1L);
            
            return true;
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
     * 本类不允许构建
     */
    private XCQLOPDDL()
    {
        
    }
    
}
