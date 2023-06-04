package org.hy.common.xcql;

import java.util.List;
import java.util.Map;





/**
 * XCQL异常信息
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-03
 * @version     v1.0
 */
public class XCQLErrorInfo
{
    
    /** 异常时执行的CQL语句 */
    private String         cql;
    
    /** 异常信息 */
    private Exception      exce;
    
    /** 异常时执行的XCQL对象 */
    private XCQL           xcql;
    
    /**
     * 异常时执行的XCQL入参数据（Map类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     */
    private Map<String ,?> valuesMap;
    
    /**
     * 异常时执行的XCQL入参数据（Object类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     */
    private Object         valuesObject;
    
    /**
     * 异常时执行的XCQL入参数据（List类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     */
    private List<?>        valuesList;
    
    
    
    public XCQLErrorInfo(String i_CQL ,Exception i_Exce ,XCQL i_XCQL)
    {
        this.cql  = i_CQL;
        this.exce = i_Exce;
        this.xcql = i_XCQL;
    }

    
    /**
     * 获取：异常时执行的CQL语句
     */
    public String getCql()
    {
        return cql;
    }

    
    /**
     * 获取：异常信息
     */
    public Exception getExce()
    {
        return exce;
    }

    
    /**
     * 获取：异常时执行的XCQL对象
     */
    public XCQL getXcql()
    {
        return xcql;
    }

    
    /**
     * 获取：* 异常时执行的XCQL入参数据（Map类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     */
    public Map<String ,?> getValuesMap()
    {
        return valuesMap;
    }

    
    /**
     * 获取：* 异常时执行的XCQL入参数据（Object类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     */
    public Object getValuesObject()
    {
        return valuesObject;
    }

    
    /**
     * 获取：* 异常时执行的XCQL入参数据（List类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     */
    public List<?> getValuesList()
    {
        return valuesList;
    }

    
    /**
     * 设置：* 异常时执行的XCQL入参数据（Map类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     * 
     * @param valuesMap
     */
    public XCQLErrorInfo setValuesMap(Map<String ,?> valuesMap)
    {
        this.valuesMap = valuesMap;
        return this;
    }

    
    /**
     * 设置：* 异常时执行的XCQL入参数据（Object类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     * 
     * @param valuesObject
     */
    public XCQLErrorInfo setValuesObject(Object valuesObject)
    {
        this.valuesObject = valuesObject;
        return this;
    }

    
    /**
     * 设置：* 异常时执行的XCQL入参数据（List类型的）。
     * 
     * valuesMap、valuesObject、valuesList三者同时只能有一个有值，或者均为NULL。
     * 
     * @param valuesList
     */
    public XCQLErrorInfo setValuesList(List<?> valuesList)
    {
        this.valuesList = valuesList;
        return this;
    }
    
}
