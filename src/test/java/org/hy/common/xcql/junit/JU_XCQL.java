package org.hy.common.xcql.junit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hy.common.Help;
import org.hy.common.xcql.XCQL;
import org.hy.common.xcql.XCQLData;
import org.hy.common.xml.XJava;
import org.hy.common.xml.annotation.XType;
import org.hy.common.xml.annotation.Xjava;
import org.hy.common.xml.log.Logger;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;





/**
 * 测试单元：XCQL基本功能的测试
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-06
 * @version     v1.0
 */
@Xjava(value=XType.XML)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JU_XCQL
{
    private static Logger  $Logger = new Logger(JU_XCQL.class ,true);
    
    private static boolean $isInit = false;
    
    
    
    public JU_XCQL() throws Exception
    {
        if ( !$isInit )
        {
            $isInit = true;
            XJava.parserAnnotation(JU_XCQL.class.getName());
        }
    }
    
    
    
    /**
     * 查询：返回List<Map>结构
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @Test
    @SuppressWarnings("unchecked")
    public void test_Query_001_ReturnListMap()
    {
        XCQL                      v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_001_ReturnListMap");
        XCQLData                  v_XCQLData = v_XCQL.queryXCQLData();
        List<Map<String ,Object>> v_Datas    = (List<Map<String ,Object>>) v_XCQLData.getDatas();
        
        for (Map<String ,Object> v_Item : v_Datas)
        {
            Help.print(v_Item);
        }
    }
    
    
    
    /**
     * 查询：返回List<Java Bean>结构
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_002_ReturnObject()
    {
        XCQL                   v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_002_ReturnObject");
        XCQLData               v_XCQLData = v_XCQL.queryXCQLData();
        List<DataSourceConfig> v_Datas    = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
    
    
    /**
     * 查询：带占位符的查询条件是对象的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_003_WhereObject()
    {
        DataSourceConfig v_Param = new DataSourceConfig();
        v_Param.setDatabaseName("dataCenter");
        
        XCQL                   v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_003_Where");
        XCQLData               v_XCQLData = v_XCQL.queryXCQLData(v_Param);
        List<DataSourceConfig> v_Datas    = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
    
    
    /**
     * 查询：带占位符的查询条件是Map的
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_004_WhereMap()
    {
        Map<String ,Object> v_Param = new HashMap<String ,Object>();
        v_Param.put("databaseName" ,"dataCenter");
        
        XCQL                   v_XCQL     = (XCQL) XJava.getObject("XCQL_Query_003_Where");
        XCQLData               v_XCQLData = v_XCQL.queryXCQLData(v_Param);
        List<DataSourceConfig> v_Datas    = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
    
    
    /**
     * 查询：分页
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-07
     * @version     v1.0
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_Query_004_Paging()
    {
        Map<String ,Object> v_Param = new HashMap<String ,Object>();
        v_Param.put("startIndex"   ,0);    // 从第几行分页。有效下标从0开始
        v_Param.put("pagePerCount" ,2);    // 每页显示数量
        
        XCQL                   v_XCQL       = (XCQL) XJava.getObject("XCQL_Query_004_Paging");
        XCQL                   v_XCQLPaging = XCQL.queryPaging(v_XCQL ,true);
        XCQLData               v_XCQLData   = v_XCQLPaging.queryXCQLData(v_Param);
        List<DataSourceConfig> v_Datas      = (List<DataSourceConfig>) v_XCQLData.getDatas();
        
        for (DataSourceConfig v_Item : v_Datas)
        {
            $Logger.info(v_Item.getDatabaseName());
        }
    }
    
}
