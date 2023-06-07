package org.hy.common.xcql.junit;

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
    
}
