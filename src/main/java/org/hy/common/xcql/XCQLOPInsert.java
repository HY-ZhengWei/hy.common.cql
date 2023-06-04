package org.hy.common.xcql;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hy.common.Date;
import org.hy.common.Help;
import org.hy.common.MethodReflect;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;





/**
 * XCQL功能中Insert语句的具体操作与实现。
 * 
 * 核心区别：它与executeUpdate方法的核心区别是：本类的所有方法将尝试返回【数据库级的自增ID】，方法返回类型为：XCQLData
 * 独立原因：从XCQL主类中分离的主要原因是：减少XCQL主类的代码量，方便维护。使XCQL主类向外提供统一的操作，本类重点关注实现。
 * 静态原因：用static方法的原因：不想再构建太多的类实例，减少内存负担
 * 接口选择：未使用接口的原因：本类的每个方法的首个入参都有一个XCQL类型，并且都是static方法
 * 
 * @author      ZhengWei(HY)
 * @createDate  2022-05-23
 * @version     v1.0
 */
public class XCQLOPInsert
{
    
    /**
     * 占位符CQL的Insert语句的执行。 -- 无填充值的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_XCQL
     * @return        返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,v_DSCQL);
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * 占位符CQL的Insert语句的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Map<String ,?> i_Values)
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
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            XCQLOPUpdate.executeUpdate_AfterWriteLob(i_XCQL ,i_Values ,(int)v_Ret.getRowCount());
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Object i_Obj)
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
            XCQLData v_Ret = XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,v_DSCQL);
            XCQLOPUpdate.executeUpdate_AfterWriteLob(i_XCQL ,i_Obj ,(int)v_Ret.getRowCount());
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 常规Insert语句的执行。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final String i_CQL)
    {
        boolean v_IsError = false;

        try
        {
            return XCQLOPInsert.executeInsert_Inner(i_XCQL ,i_CQL ,i_XCQL.getDataSourceCQL());
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * 常规Insert语句的执行。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @return                   返回语句影响的记录数及自增长ID。
     */
    private static XCQLData executeInsert_Inner(final XCQL i_XCQL ,final String i_CQL ,final DataSourceCQL i_DSG)
    {
        Session v_Conn      = null;
        long    v_BeginTime = i_XCQL.request().getTime();
        
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
            
            v_Conn = i_XCQL.getConnection(i_DSG);
            
            Result v_Result = v_Conn.run(i_CQL);
            i_XCQL.log(i_CQL);
            
            if ( v_Count >= 1 )
            {
                v_Identitys = XCQLOPInsert.readIdentitys(v_Statement);
            }
            
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,1 ,v_Count);
            
            return new XCQLData(v_Identitys ,v_Count ,1 ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(null ,v_Statement ,v_Conn);
        }
    }
    
    
    
    /**
     * 占位符CQL的Insert语句的执行。 -- 无填充值的（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean         v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String          v_CQL     = null;

        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(v_DSCQL);
            return XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,i_Conn);
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * 占位符CQL的Insert语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Map<String ,?> i_Values ,final Connection i_Conn)
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
            return XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,i_Conn);
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * 占位符CQL的Insert语句的执行。（内部不再关闭数据库连接）
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final Object i_Obj ,final Connection i_Conn)
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
            return XCQLOPInsert.executeInsert_Inner(i_XCQL ,v_CQL ,i_Conn);
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * 常规Insert语句的执行。（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsert(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        boolean v_IsError = false;

        try
        {
            return XCQLOPInsert.executeInsert_Inner(i_XCQL ,i_CQL ,i_Conn);
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
     * 常规Insert语句的执行。（内部不再关闭数据库连接）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-05-23
     * @version     v3.0
     * 
     * @param i_CQL              常规CQL语句
     * @param i_Conn             数据库连接
     * @return                   返回语句影响的记录数及自增长ID。
     */
    private static XCQLData executeInsert_Inner(final XCQL i_XCQL ,final String i_CQL ,final Connection i_Conn)
    {
        Statement v_Statement = null;
        long      v_BeginTime = i_XCQL.request().getTime();
        
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
            
            v_Statement = i_Conn.createStatement();
            
            int           v_Count     = v_Statement.executeUpdate(i_CQL ,Statement.RETURN_GENERATED_KEYS);
            List<Integer> v_Identitys = null;
            i_XCQL.log(i_CQL);
            
            if ( v_Count >= 1 )
            {
                v_Identitys = XCQLOPInsert.readIdentitys(v_Statement);
            }
            
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,1 ,v_Count);
            
            return new XCQLData(v_Identitys ,v_Count ,1 ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            XCQL.erroring(i_CQL ,exce ,i_XCQL);
            throw new RuntimeException(exce.getMessage());
        }
        finally
        {
            i_XCQL.closeDB(null ,v_Statement ,null);
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句的执行。
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
        
        boolean v_IsError = false;

        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            return XCQLOPInsert.executeInserts_Inner(i_XCQL ,i_ObjList ,null);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executeUpdates(i_ObjList);
            }
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句的执行。
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
        
        boolean v_IsError = false;

        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            return XCQLOPInsert.executeInserts_Inner(i_XCQL ,i_ObjList ,i_Conn);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executeUpdates(i_ObjList);
            }
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句的执行。
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
    private static XCQLData executeInserts_Inner(final XCQL i_XCQL ,final List<?> i_ObjList ,final Connection i_Conn)
    {
        DataSourceCQL v_DSCQL        = null;
        Connection      v_Conn       = null;
        Statement       v_Statement  = null;
        boolean         v_AutoCommit = false;
        int             v_Ret        = 0;
        long            v_BeginTime  = i_XCQL.request().getTime();
        String          v_CQL        = null;
        int             v_CQLCount   = 0;
        List<Integer>   v_Identitys  = null;
        
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
            
            v_Conn       = i_Conn == null ? i_XCQL.getConnection(v_DSCQL) : i_Conn;
            v_AutoCommit = v_Conn.getAutoCommit();
            v_Conn.setAutoCommit(false);
            v_Statement  = v_Conn.createStatement();
            v_Identitys  = new ArrayList<Integer>();
            
            if ( i_XCQL.getBatchCommit() <= 0 )
            {
                for (int i=0; i<i_ObjList.size(); i++)
                {
                    if ( i_ObjList.get(i) != null )
                    {
                        v_CQL       = i_XCQL.getContent().getCQL(i_ObjList.get(i) ,v_DSCQL);
                        v_CQLCount = v_Statement.executeUpdate(v_CQL ,Statement.RETURN_GENERATED_KEYS);
                        if ( v_CQLCount >= 1 )
                        {
                            v_Ret += v_CQLCount;
                            XCQLOPInsert.readIdentitys(v_Statement ,v_Identitys);
                        }
                        i_XCQL.log(v_CQL);
                    }
                }
                
                if ( i_Conn == null )
                {
                    v_Conn.commit();  // 它与i_Conn.commit();同作用
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
                        v_CQLCount = v_Statement.executeUpdate(v_CQL ,Statement.RETURN_GENERATED_KEYS);
                        if ( v_CQLCount >= 1 )
                        {
                            v_Ret += v_CQLCount;
                            XCQLOPInsert.readIdentitys(v_Statement ,v_Identitys);
                        }
                        i_XCQL.log(v_CQL);
                        v_EC++;
                        
                        if ( v_EC % i_XCQL.getBatchCommit() == 0 )
                        {
                            v_Conn.commit();
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
                    v_Conn.commit();
                }
            }
            
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,i_ObjList.size() ,v_Ret);
            return new XCQLData(v_Identitys ,v_Ret ,1 ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            XCQL.erroring(v_CQL ,exce ,i_XCQL);
            
            try
            {
                if ( i_Conn == null && v_Conn != null )
                {
                    v_Conn.rollback();
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
                try
                {
                    if ( v_Conn != null )
                    {
                        v_Conn.setAutoCommit(v_AutoCommit);
                    }
                }
                catch (Exception exce)
                {
                    // Nothing.
                }
                
                i_XCQL.closeDB(null ,v_Statement ,v_Conn);
            }
            else
            {
                i_XCQL.closeDB(null ,v_Statement ,null);
            }
        }
    }
    
    
    
    /**
     * 一行数据的批量执行：占位符CQL的Insert语句的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-04-20
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsertPrepared(final XCQL i_XCQL ,final Map<String ,?> i_Values)
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
            XCQLData v_Ret = XCQLOPInsert.executeInsertPrepared_Inner(i_XCQL ,i_Values ,null);
            XCQLOPUpdate.executeUpdate_AfterWriteLob(i_XCQL ,i_Values ,(int)v_Ret.getRowCount());
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 一行数据的批量执行：占位符CQL的Insert语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-04-20
     * @version     v3.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsertPrepared(final XCQL i_XCQL ,final Object i_Obj)
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
            XCQLData v_Ret = XCQLOPInsert.executeInsertPrepared_Inner(i_XCQL ,i_Obj ,null);
            XCQLOPUpdate.executeUpdate_AfterWriteLob(i_XCQL ,i_Obj ,(int)v_Ret.getRowCount());
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 一行数据的批量执行：占位符CQL的Insert语句的执行。
     * 
     * 1. 按集合 Map<String ,Object> 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-04-20
     * @version     v3.0
     * 
     * @param i_Values           占位符CQL的填充集合。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsertPrepared(final XCQL i_XCQL ,final Map<String ,?> i_Values ,final Connection i_Conn)
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
            XCQLData v_Ret = XCQLOPInsert.executeInsertPrepared_Inner(i_XCQL ,i_Values ,i_Conn);
            XCQLOPUpdate.executeUpdate_AfterWriteLob(i_XCQL ,i_Values ,(int)v_Ret.getRowCount());
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 一行数据的批量执行：占位符CQL的Insert语句的执行。
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * V2.0  2018-07-18  1.添加：支持CLob字段类型的简单Insert的写入操作。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-04-20
     * @version     v3.0
     * 
     * @param i_Obj              占位符CQL的填充对象。
     * @return                   返回语句影响的记录数及自增长ID。
     */
    public static XCQLData executeInsertPrepared(final XCQL i_XCQL ,final Object i_Obj ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean       v_IsError = false;
        DataSourceCQL v_DSCQL     = null;
        String        v_CQL     = null;

        try
        {
            i_XCQL.fireBeforeRule(i_Obj);
            v_DSCQL = i_XCQL.getDataSourceCQL();
            v_CQL = i_XCQL.getContent().getCQL(i_Obj ,v_DSCQL);
            XCQLData v_Ret = XCQLOPInsert.executeInsertPrepared_Inner(i_XCQL ,i_Obj ,i_Conn);
            XCQLOPUpdate.executeUpdate_AfterWriteLob(i_XCQL ,i_Obj ,(int)v_Ret.getRowCount());
            return v_Ret;
        }
        /* try{}已有中捕获所有异常，并仅出外抛出Null和Runtime两种异常。为保持异常类型不变，写了两遍一样的 */
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
                i_XCQL.getTrigger().executes();
            }
        }
    }
    
    
    
    /**
     * 一行数据的批量执行：占位符CQL的Insert语句的执行。
     * 
     *   注意：不支持Delete语句
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-04-20
     * @version     v1.0
     * 
     * @param i_ObjList          占位符CQL的填充对象。
     * @param i_Conn             数据库连接。
     *                           1. 当为空时，内部自动获取一个新的数据库连接。
     *                           2. 当有值时，内部将不关闭数据库连接，而是交给外部调用者来关闭。
     *                           3. 当有值时，内部也不执行"提交"操作（但分批提交this.batchCommit大于0时除外），而是交给外部调用者来执行"提交"。
     *                           4. 当有值时，出现异常时，内部也不执行"回滚"操作，而是交给外部调用者来执行"回滚"。
     * @return                   返回语句影响的记录数。
     */
    @SuppressWarnings("unchecked")
    private static XCQLData executeInsertPrepared_Inner(final XCQL i_XCQL ,final Object i_Obj ,final Connection i_Conn)
    {
        DataSourceCQL   v_DSCQL         = null;
        Connection        v_Conn        = null;
        PreparedStatement v_PStatement  = null;
        boolean           v_AutoCommit  = false;
        int               v_Ret         = 0;
        int               v_CommitCount = 0;
        long              v_BeginTime   = i_XCQL.request().getTime();
        String            v_CQL         = null;
        List<Integer>     v_Identitys   = null;
        
        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            if ( !v_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + v_DSCQL.getXJavaID() + "] is not valid..");
            }
            
            if ( i_Obj == null )
            {
                throw new NullPointerException("Batch execute update Object is null.");
            }
            
            v_Conn       = i_Conn == null ? i_XCQL.getConnection(v_DSCQL) : i_Conn;
            v_AutoCommit = v_Conn.getAutoCommit();
            v_Conn.setAutoCommit(false);
            v_CQL        = i_XCQL.getContent().getPreparedCQL().getCQL();
            v_PStatement = v_Conn.prepareStatement(v_CQL ,Statement.RETURN_GENERATED_KEYS);
            v_Identitys  = new ArrayList<Integer>();
            
            if ( MethodReflect.isExtendImplement(i_Obj ,Map.class) )
            {
                int v_ParamIndex = 0;
                for (String v_PlaceHolder : i_XCQL.getContent().getPreparedCQL().getPlaceholders())
                {
                    Object v_Value = MethodReflect.getMapValue((Map<String ,?>)i_Obj ,v_PlaceHolder);
                    
                    XCQLOPUpdate.preparedStatementSetValue(v_PStatement ,++v_ParamIndex ,v_Value ,null);
                }
            }
            else
            {
                int v_ParamIndex = 0;
                for (String v_PlaceHolder : i_XCQL.getContent().getPreparedCQL().getPlaceholders())
                {
                    MethodReflect v_MethodReflect = new MethodReflect(i_Obj ,v_PlaceHolder ,true ,MethodReflect.$NormType_Getter);
                    
                    XCQLOPUpdate.preparedStatementSetValue(v_PStatement ,++v_ParamIndex ,v_MethodReflect.invoke() ,v_MethodReflect.getReturnType());
                    
                    v_MethodReflect.clearDestroy();
                    v_MethodReflect = null;
                }
            }
            
            v_PStatement.addBatch();
            
            int [] v_CountArr = v_PStatement.executeBatch();
            XCQLOPInsert.readIdentitys(v_PStatement ,v_Identitys);
            
            if ( i_Conn == null )
            {
                v_Conn.commit();  // 它与i_Conn.commit();同作用
                v_CommitCount++;
            }
            
            for (int v_Count : v_CountArr)
            {
                if ( v_Count >= 1 )
                {
                    v_Ret += v_Count;
                }
                else if ( Statement.SUCCESS_NO_INFO == v_Count )
                {
                    // 执行成功了，但不知道影响的行数
                    v_Ret++;
                }
            }
            
            i_XCQL.log(v_CQL);
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,v_CommitCount ,v_Ret);
            
            return new XCQLData(v_Identitys ,v_Ret ,1 ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            v_CQL = i_XCQL.getContent().getPreparedCQL().getCQL(i_Obj);
            XCQL.erroring(v_CQL ,exce ,i_XCQL);
            
            try
            {
                if ( i_Conn == null && v_Conn != null )
                {
                    v_Conn.rollback();
                }
            }
            catch (Exception e)
            {
                // Nothing.
            }
            
            throw new RuntimeException(exce.getMessage() + "：" + v_CQL);
        }
        finally
        {
            if ( i_Conn == null )
            {
                try
                {
                    if ( v_Conn != null )
                    {
                        v_Conn.setAutoCommit(v_AutoCommit);
                    }
                }
                catch (Exception exce)
                {
                    // Nothing.
                }
                
                i_XCQL.closeDB(null ,v_PStatement ,v_Conn);
            }
            else
            {
                i_XCQL.closeDB(null ,v_PStatement ,null);
            }
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句的执行。
     * 
     *   注意：不支持Delete语句
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-03
     * @version     v1.0
     *              v2.0  2022-05-24  1. 添加：支持自增长ID的获取及返回
     * 
     * @param i_ObjList          占位符CQL的填充对象的集合。
     *                           1. 集合元素可以是Object
     *                           2. 集合元素可以是Map<String ,?>
     *                           3. 更可以是上面两者的混合元素组成的集合
     * @return                   返回语句影响的记录数。
     */
    public static XCQLData executeInsertsPrepared(final XCQL i_XCQL ,final List<?> i_ObjList)
    {
        i_XCQL.checkContent();
        
        boolean v_IsError = false;

        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            return XCQLOPInsert.executeInsertsPrepared_Inner(i_XCQL ,i_ObjList ,null);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executeUpdatesPrepared(i_ObjList);
            }
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句的执行。
     * 
     *   注意：不支持Delete语句
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-03
     * @version     v1.0
     *              v2.0  2022-05-24  1. 添加：支持自增长ID的获取及返回
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
    public static XCQLData executeInsertsPrepared(final XCQL i_XCQL ,final List<?> i_ObjList ,final Connection i_Conn)
    {
        i_XCQL.checkContent();
        
        boolean v_IsError = false;
        
        try
        {
            i_XCQL.fireBeforeRule(i_ObjList);
            return XCQLOPInsert.executeInsertsPrepared_Inner(i_XCQL ,i_ObjList ,i_Conn);
        }
        catch (NullPointerException exce)
        {
            v_IsError = true;
            if ( i_XCQL.getError() != null )
            {
                i_XCQL.getError().errorLog(new XCQLErrorInfo(i_XCQL.getContent().getCQL(i_XCQL.getDataSourceCQL()) ,exce ,i_XCQL).setValuesList(i_ObjList));
            }
            throw exce;
        }
        catch (RuntimeException exce)
        {
            v_IsError = true;
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
                i_XCQL.getTrigger().executeUpdatesPrepared(i_ObjList);
            }
        }
    }
    
    
    
    /**
     * 批量执行：占位符CQL的Insert语句的执行。
     * 
     *   注意：不支持Delete语句
     * 
     * 1. 按对象 i_Obj 填充占位符CQL，生成可执行的CQL语句；
     * 
     * 注：只支持单一CQL语句的执行
     * 
     * @author      ZhengWei(HY)
     * @createDate  2016-08-03
     * @version     v1.0
     * @version     v2.0  2022-05-18  1. 修改：统计数据中的 "请求数" ，从原来的集合元素个数，调整为提交次数
     *              v3.0  2022-05-24  1. 添加：支持自增长ID的获取及返回
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
    @SuppressWarnings("unchecked")
    private static XCQLData executeInsertsPrepared_Inner(final XCQL i_XCQL ,final List<?> i_ObjList ,final Connection i_Conn)
    {
        DataSourceCQL     v_DSCQL       = null;
        Connection        v_Conn        = null;
        PreparedStatement v_PStatement  = null;
        boolean           v_AutoCommit  = false;
        int               v_Ret         = 0;
        int               v_CommitCount = 0;
        long              v_BeginTime   = i_XCQL.request().getTime();
        String            v_CQL         = null;
        List<Integer>     v_Identitys   = null;
        Object            v_Object      = null;
        
        try
        {
            v_DSCQL = i_XCQL.getDataSourceCQL();
            if ( !v_DSCQL.isValid() )
            {
                throw new RuntimeException("DataSourceCQL[" + v_DSCQL.getXJavaID() + "] is not valid..");
            }
            
            if ( Help.isNull(i_ObjList) )
            {
                throw new NullPointerException("Batch execute update List<Object> is null.");
            }
            
            v_Conn       = i_Conn == null ? i_XCQL.getConnection(v_DSCQL) : i_Conn;
            v_AutoCommit = v_Conn.getAutoCommit();
            v_Conn.setAutoCommit(false);
            v_CQL        = i_XCQL.getContent().getPreparedCQL().getCQL();
            v_PStatement = v_Conn.prepareStatement(v_CQL ,Statement.RETURN_GENERATED_KEYS);
            v_Identitys  = new ArrayList<Integer>();
            
            if ( i_XCQL.getBatchCommit() <= 0 )
            {
                for (int i=0; i<i_ObjList.size(); i++)
                {
                    v_Object = i_ObjList.get(i);
                    if ( v_Object != null )
                    {
                        if ( MethodReflect.isExtendImplement(v_Object ,Map.class) )
                        {
                            int v_ParamIndex = 0;
                            for (String v_PlaceHolder : i_XCQL.getContent().getPreparedCQL().getPlaceholders())
                            {
                                Object v_Value = MethodReflect.getMapValue((Map<String ,?>)v_Object ,v_PlaceHolder);
                                
                                XCQLOPUpdate.preparedStatementSetValue(v_PStatement ,++v_ParamIndex ,v_Value ,null);
                            }
                        }
                        else
                        {
                            int v_ParamIndex = 0;
                            for (String v_PlaceHolder : i_XCQL.getContent().getPreparedCQL().getPlaceholders())
                            {
                                MethodReflect v_MethodReflect = new MethodReflect(v_Object ,v_PlaceHolder ,true ,MethodReflect.$NormType_Getter);
                                
                                XCQLOPUpdate.preparedStatementSetValue(v_PStatement ,++v_ParamIndex ,v_MethodReflect.invoke() ,v_MethodReflect.getReturnType());
                                
                                v_MethodReflect.clearDestroy();
                                v_MethodReflect = null;
                            }
                        }
                        
                        v_PStatement.addBatch();
                    }
                }
                
                int [] v_CountArr = v_PStatement.executeBatch();
                XCQLOPInsert.readIdentitys(v_PStatement ,v_Identitys);
                
                if ( i_Conn == null )
                {
                    v_Conn.commit();  // 它与i_Conn.commit();同作用
                    v_CommitCount++;
                }
                
                for (int v_Count : v_CountArr)
                {
                    if ( v_Count >= 1 )
                    {
                        v_Ret += v_Count;
                    }
                    else if ( Statement.SUCCESS_NO_INFO == v_Count )
                    {
                        // 执行成功了，但不知道影响的行数
                        v_Ret++;
                    }
                }
            }
            else
            {
                boolean v_IsCommit = true;  // 2017-11-06  修正：当预处理 this.executeUpdatesPrepared_Inner() 执行的同时 batchCommit >= 1时，可能出现未"执行executeBatch"情况
                
                for (int i=0 ,v_EC=0; i<i_ObjList.size(); i++)
                {
                    v_Object = i_ObjList.get(i);
                    if ( v_Object != null )
                    {
                        if ( MethodReflect.isExtendImplement(v_Object ,Map.class) )
                        {
                            int v_ParamIndex = 0;
                            for (String v_PlaceHolder : i_XCQL.getContent().getPreparedCQL().getPlaceholders())
                            {
                                Object v_Value = MethodReflect.getMapValue((Map<String ,?>)v_Object ,v_PlaceHolder);
                                
                                XCQLOPUpdate.preparedStatementSetValue(v_PStatement ,++v_ParamIndex ,v_Value ,null);
                            }
                        }
                        else
                        {
                            int v_ParamIndex = 0;
                            for (String v_PlaceHolder : i_XCQL.getContent().getPreparedCQL().getPlaceholders())
                            {
                                MethodReflect v_MethodReflect = new MethodReflect(v_Object ,v_PlaceHolder ,true ,MethodReflect.$NormType_Getter);
                                
                                XCQLOPUpdate.preparedStatementSetValue(v_PStatement ,++v_ParamIndex ,v_MethodReflect.invoke() ,v_MethodReflect.getReturnType());
                                
                                v_MethodReflect.clearDestroy();
                                v_MethodReflect = null;
                            }
                        }
                        
                        v_PStatement.addBatch();
                        v_EC++;
                        
                        if ( v_EC % i_XCQL.getBatchCommit() == 0 )
                        {
                            int [] v_CountArr = v_PStatement.executeBatch();
                            XCQLOPInsert.readIdentitys(v_PStatement ,v_Identitys);
                            v_Conn.commit();
                            v_CommitCount++;
                            
                            for (int v_Count : v_CountArr)
                            {
                                if ( v_Count >= 1 )
                                {
                                    v_Ret += v_Count;
                                }
                                else if ( Statement.SUCCESS_NO_INFO == v_Count )
                                {
                                    // 执行成功了，但不知道影响的行数
                                    v_Ret++;
                                }
                            }
                            
                            v_PStatement.clearBatch();
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
                    int [] v_CountArr = v_PStatement.executeBatch();
                    XCQLOPInsert.readIdentitys(v_PStatement ,v_Identitys);
                    v_Conn.commit();
                    v_CommitCount++;
                    
                    for (int v_Count : v_CountArr)
                    {
                        if ( v_Count >= 1 )
                        {
                            v_Ret += v_Count;
                        }
                        else if ( Statement.SUCCESS_NO_INFO == v_Count )
                        {
                            // 执行成功了，但不知道影响的行数
                            v_Ret++;
                        }
                    }
                }
            }
            
            i_XCQL.log(v_CQL);
            Date v_EndTime = Date.getNowTime();
            long v_TimeLen = v_EndTime.getTime() - v_BeginTime;
            i_XCQL.success(v_EndTime ,v_TimeLen ,v_CommitCount ,v_Ret);
            
            return new XCQLData(v_Identitys ,v_Ret ,1 ,v_TimeLen ,null);
        }
        catch (Exception exce)
        {
            v_CQL = i_XCQL.getContent().getPreparedCQL().getCQL(v_Object);
            XCQL.erroring(v_CQL ,exce ,i_XCQL);
            
            try
            {
                if ( i_Conn == null && v_Conn != null )
                {
                    v_Conn.rollback();
                }
            }
            catch (Exception e)
            {
                // Nothing.
            }
            
            throw new RuntimeException(exce.getMessage() + "：" + v_CQL);
        }
        finally
        {
            if ( i_Conn == null )
            {
                try
                {
                    if ( v_Conn != null )
                    {
                        v_Conn.setAutoCommit(v_AutoCommit);
                    }
                }
                catch (Exception exce)
                {
                    // Nothing.
                }
                
                i_XCQL.closeDB(null ,v_PStatement ,v_Conn);
            }
            else
            {
                i_XCQL.closeDB(null ,v_PStatement ,null);
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
