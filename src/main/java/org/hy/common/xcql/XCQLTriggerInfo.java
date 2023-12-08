package org.hy.common.xcql;





/**
 * 触发器的元素对象
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-03
 * @version     v1.0
 */
public class XCQLTriggerInfo
{
    /** XCQL对象 */
    private XCQL xcql;
   
    /** 执行类型（0按execute方法执行，1按executeUpdate方法执行） */
    private int  executeType;
    
    
    
    public XCQLTriggerInfo(XCQL i_XCQL ,int i_ExecuteType)
    {
        this.xcql        = i_XCQL;
        this.executeType = i_ExecuteType;
    }

    
    /**
     * 获取：XCQL对象
     */
    public XCQL getXcql()
    {
        return xcql;
    }
    
    
    /**
     * 设置：XCQL对象
     * 
     * @param i_Xcql XCQL对象
     */
    public void setXcql(XCQL i_Xcql)
    {
        this.xcql = i_Xcql;
    }


    /**
     * 获取：执行类型（0按execute方法执行，1按executeUpdate方法执行）
     */
    public int getExecuteType()
    {
        return executeType;
    }

    
    /**
     * 设置：执行类型（0按execute方法执行，1按executeUpdate方法执行）
     * 
     * @param executeType
     */
    public void setExecuteType(int executeType)
    {
        this.executeType = executeType;
    }
    
}
