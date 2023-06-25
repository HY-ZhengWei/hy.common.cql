package org.hy.common.xcql;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hy.common.Date;
import org.hy.common.Help;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;





/**
 * XCQL功能中MATCH语句的具体操作与实现。
 * 
 * 独立原因：从XCQL主类中分离的主要原因是：减少XCQL主类的代码量，方便维护。使XCQL主类向外提供统一的操作，本类重点关注实现。
 * 静态原因：用static方法的原因：不想再构建太多的类实例，减少内存负担
 * 接口选择：未使用接口的原因：本类的每个方法的首个入参都有一个XCQL类型，并且都是static方法
 * 
 * @author      ZhengWei(HY)
 * @createDate  2022-06-04
 * @version     v1.0
 */
public class XCQLOPQuery
{
    
    /** 判定查询QL中否有COUNT(1)或COUNT(*)的情况 */
    private static final String $CQLHaveCount = "( )+[Cc][Oo][Uu][Nn][Tt][ ]*(\\()+";
    
    
    
    /**
     * 占位符CQL的查询。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Conn
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 占位符CQL的查询。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Conn
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,i_Conn);
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
     * 占位符CQL的查询。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final Map<String ,?> i_Values)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;
        
        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 占位符CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final Map<String ,?> i_Values ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;
        
        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,i_Conn);
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
     * 占位符CQL的查询。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final Object i_Obj)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 占位符CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final Object i_Obj ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,i_Conn);
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
     * 常规CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean v_IsError = false;

        try
        {
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,i_CQL ,i_Conn);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger();
            }
        }
    }
    
    
    
    /**
     * 常规CQL的查询。
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final String i_CQL)
    {
        i_XCQL.checkContent();
        
        boolean v_IsError = false;

        try
        {
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,i_CQL ,i_XCQL.getDataSourceCQL());
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger();
            }
        }
    }
    
    
    
    /**
     * 常规CQL的查询。（内部不再关闭数据库连接）
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_CQL              常规CQL语句
     * @return
     */
    private static XCQLData queryXCQLData_Inner(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        Result v_Resultset = null;
        long   v_BeginTime = i_XCQL.request().getTime();
        
        try
        {
            if ( i_XCQL.getResult() == null )
            {
                throw new NullPointerException("Result is null of XCQL.");
            }
            
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            if ( null == i_Conn)
            {
                throw new NullPointerException("Connection is null of XCQL.");
            }
            
            v_Resultset = i_Conn.run(i_CQL);
            i_XCQL.log(i_CQL);
            
            XCQLData v_Ret = i_XCQL.getResult().getDatas(v_Resultset);
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,v_Ret.getRowCount());
            
            i_XCQL.fireAfterRule(v_Ret);
            
            return v_Ret;
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(v_Resultset ,null);
        }
    }
    
    
    
    /**
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final int i_StartRow ,final int i_PagePerSize)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,v_DSCQL ,i_StartRow ,i_PagePerSize);
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
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final Map<String ,?> i_Values ,final int i_StartRow ,final int i_PagePerSize)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,v_DSCQL ,i_StartRow ,i_PagePerSize);
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
     * 占位符CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final Object i_Obj ,final int i_StartRow ,final int i_PagePerSize)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,v_CQL ,v_DSCQL ,i_StartRow ,i_PagePerSize);
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
     * 常规CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     *
     * @param i_CQL              常规CQL语句
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    public static XCQLData queryXCQLData(final XCQL i_XCQL ,final String i_CQL ,final int i_StartRow ,final int i_PagePerSize)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        
        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            return XCQLOPQuery.queryXCQLData_Inner(i_XCQL ,i_CQL ,v_DSCQL ,i_StartRow ,i_PagePerSize);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_CQL ,exce ,i_XCQL));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 常规CQL的查询。游标的分页查询（可通用于所有数据库）。
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     *
     * @param i_CQL              常规CQL语句
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    private static XCQLData queryXCQLData_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSCQL ,final int i_StartRow ,final int i_PagePerSize)
    {
        Connection v_Conn      = null;
        Result     v_Resultset = null;
        long       v_BeginTime = i_XCQL.request().getTime();
        
        try
        {
            if ( i_XCQL.getResult() == null )
            {
                throw new NullPointerException("Result is null of XCQL.");
            }
            
            if ( !i_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + i_DSCQL.getXJavaID() + "] is not valid.");
            }
            
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            v_Conn      = i_XCQL.getConnection(i_DSCQL);
            v_Resultset = v_Conn.run(i_CQL);
            i_XCQL.log(i_CQL);
            
            XCQLData v_Ret = i_XCQL.getResult().getDatas(v_Resultset ,i_StartRow ,i_PagePerSize);
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,v_Ret.getRowCount());
            
            i_XCQL.fireAfterRule(v_Ret);
            
            return v_Ret;
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(v_Resultset ,v_Conn);
        }
    }
    
    
    
    /**
     * 常规CQL的查询
     * 
     * 1. 提交数据库执行 i_CQL ，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-22
     * @version     v1.0
     * 
     * @param i_CQL  常规CQL语句
     * @return
     */
    private static XCQLData queryXCQLData_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSCQL)
    {
        Connection v_Conn      = null;
        Result     v_Resultset = null;
        long       v_BeginTime = i_XCQL.request().getTime();
        
        try
        {
            if ( i_XCQL.getResult() == null )
            {
                throw new NullPointerException("Result is null of XCQL.");
            }
            
            if ( !i_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + i_DSCQL.getXJavaID() + "] is not valid.");
            }
            
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            v_Conn      = i_XCQL.getConnection(i_DSCQL);
            v_Resultset = v_Conn.run(i_CQL);
            i_XCQL.log(i_CQL);
            
            XCQLData v_Ret = i_XCQL.getResult().getDatas(v_Resultset);
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,v_Ret.getRowCount());
            
            i_XCQL.fireAfterRule(v_Ret);
            
            return v_Ret;
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(v_Resultset ,v_Conn);
        }
    }
    
    
    
    /**
     * 统计记录数据：占位符CQL的查询。
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_XCQL    查询对象
     * @param i_Values  占位符CQL的填充集合。
     * @return
     */
    public static long queryCQLCount(final XCQL i_XCQL ,final Map<String ,?> i_Values)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            return XCQLOPQuery.queryCQLCount_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 统计记录数据：占位符CQL的查询。
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @param i_XCQL  查询对象
     * @param i_Obj   占位符CQL的填充对象。
     * @return
     */
    public static long queryCQLCount(final XCQL i_XCQL ,final Object i_Obj)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            return XCQLOPQuery.queryCQLCount_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 查询记录总数
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * @param i_XCQL  查询对象
     * @return
     */
    public static long queryCQLCount(final XCQL i_XCQL)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPQuery.queryCQLCount_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 查询记录总数
     * 
     * 模块CQL的形式如：SELECT COUNT(1) FROM ...
     * 
     * @param i_XCQL  查询对象
     * @param i_CQL   查询CQL
     * @return
     */
    public static long queryCQLCount(final XCQL i_XCQL ,final String i_CQL)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPQuery.queryCQLCount_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 查询记录总数
     * 
     * 模块CQL的形式如：MATCH (n) RETURN COUNT(n)
     *
     * @param i_XCQL  查询对象
     * @param i_CQL   查询CQL
     * @param i_DSCQL   查询数据源连接池组
     * @return
     */
    private static long queryCQLCount_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSCQL)
    {
        Connection v_Conn      = null;
        Result     v_Resultset = null;
        long       v_CQLCount  = 0;
        long       v_BeginTime = i_XCQL.request().getTime();
        
        try
        {
            if ( !i_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + i_DSCQL.getXJavaID() + "] is not valid.");
            }
            
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            // XCQL getCQLCount() 自行判定是否有count(1) or count(*)
            Pattern v_Pattern = null;
            Matcher v_Matcher = null;
            
            v_Pattern = Pattern.compile($CQLHaveCount);
            v_Matcher = v_Pattern.matcher(i_CQL);
            if ( !v_Matcher.find() )
            {
                throw new RuntimeException("XCQL.queryCQLCount()'s CQL is not find COUNT(1) or COUNT(*).");
            }
            
            v_Conn      = i_XCQL.getConnection(i_DSCQL);
            v_Resultset = v_Conn.run(i_CQL);
            i_XCQL.log(i_CQL);
            
            if ( v_Resultset.hasNext() )
            {
                Record v_Record = v_Resultset.next();
                Object v_Value  = XCQLMethod.getValue(v_Record.get(0));
                
                if ( v_Value != null )
                {
                    if ( v_Value instanceof Long )
                    {
                        v_CQLCount = (Long) v_Value;
                    }
                    else if ( v_Value instanceof Integer )
                    {
                        v_CQLCount = ((Integer) v_Value).longValue();
                    }
                }
            }
            
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,1L);
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(v_Resultset ,v_Conn);
        }
        
        
        return v_CQLCount;
    }
    
    
    
    /**
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * @param i_XCQL    查询对象
     * @param i_Values  占位符CQL的填充集合。
     * @return
     * @throws Exception
     */
    public static Object queryCQLValue(final XCQL i_XCQL ,final Map<String ,?> i_Values)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Values);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Values ,v_DSCQL);
            return XCQLOPQuery.queryCQLValue_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 2. 并提交数据库执行CQL，将数据库结果集转化为Java实例对象返回
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * @param i_XCQL  查询对象
     * @param i_Obj   占位符CQL的填充对象。
     * @return
     * @throws Exception
     */
    public static Object queryCQLValue(final XCQL i_XCQL ,final Object i_Obj)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            return XCQLOPQuery.queryCQLValue_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * @param i_XCQL  查询对象
     * @return
     */
    public static Object queryCQLValue(final XCQL i_XCQL)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPQuery.queryCQLValue_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     * 
     * @param i_XCQL  查询对象
     * @param i_CQL   查询CQL
     * @return
     */
    public static Object queryCQLValue(final XCQL i_XCQL ,final String i_CQL)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL   = null;
        String        v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPQuery.queryCQLValue_Inner(i_XCQL ,v_CQL ,v_DSCQL);
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
     * 查询返回第一行第一列上的数值。常用于查询返回一个字符串
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-03-07
     * @version     v1.0
     *
     * @param i_XCQL   查询对象
     * @param i_CQL    查询CQL
     * @param i_DSCQL    数据库连接池组
     * @return
     */
    private static Object queryCQLValue_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSCQL)
    {
        Connection v_Conn      = null;
        Result     v_Resultset = null;
        Object     v_CQLValue  = null;
        long       v_BeginTime = i_XCQL.request().getTime();

        
        try
        {
            if ( !i_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + i_DSCQL.getXJavaID() + "] is not valid.");
            }
            
            if ( Help.isNull(i_CQL) )
            {
                throw new NullPointerException("CQL or CQL-Params is null of XCQL.");
            }
            
            v_Conn      = i_XCQL.getConnection(i_DSCQL);
            v_Resultset = v_Conn.run(i_CQL);
            i_XCQL.log(i_CQL);
            
            if ( v_Resultset.hasNext() )
            {
                Record v_Record = v_Resultset.next();
                v_CQLValue = XCQLMethod.getValue(v_Record.get(0));
            }
            
            Date v_EndTime = Date.getNowTime();
            i_XCQL.success(v_EndTime ,v_EndTime.getTime() - v_BeginTime ,1 ,1L);
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(v_Resultset ,v_Conn);
        }
        
        
        return v_CQLValue;
    }
    
    
    
    /**
     * 本类不允许构建
     */
    private XCQLOPQuery()
    {
        
    }
    
}
