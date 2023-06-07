package org.hy.common.xcql;





/**
 * 查询数据库返回的结果。
 * 
 * 主要是通过 XCQLResullt.getDatas() 方法返回的结果。
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public class XCQLData
{
    
    /** 查询返回的结果 */
    private Object          datas;
    
    /** 将数据库结果集转化为Java实例对象的行数(已读取的行数) */
    private long            rowCount;
    
    /** 将数据库结果集转化为Java实例对象的列数(有效列数) */
    private int             colCount;
    
    /** 将数据库结果集转化为Java实例对象的关系的数量 */
    private long            relCount;
    
    /** 将数据库结果集转化为Java实例对象的用时时长(单位：毫秒) */
    private long            timeLen;
    
    /** 结果集的字段结构 */
    private DBTableMetaData metaData;
    
    
    
    public XCQLData(Object i_Datas ,long i_RowCount ,int i_ColCount ,long i_RelCount,long i_TimeLen ,DBTableMetaData i_MetaData)
    {
        this.datas     = i_Datas;
        this.rowCount  = i_RowCount;
        this.colCount  = i_ColCount;
        this.relCount  = i_RelCount;
        this.timeLen   = i_TimeLen;
        this.metaData  = i_MetaData;
    }

    
    
    /**
     * 获取：查询返回的结果
     */
    public Object getDatas()
    {
        return datas;
    }


    
    /**
     * 获取：将数据库结果集转化为Java实例对象的行数(已读取的行数)
     */
    public long getRowCount()
    {
        return rowCount;
    }


    
    /**
     * 获取：将数据库结果集转化为Java实例对象的列数(有效列数)
     */
    public int getColCount()
    {
        return colCount;
    }


    
    /**
     * 获取：将数据库结果集转化为Java实例对象的用时时长(单位：毫秒)
     */
    public long getTimeLen()
    {
        return timeLen;
    }

    
    
    /**
     * 获取：结果集的字段结构
     */
    public DBTableMetaData getMetaData()
    {
        return metaData;
    }


    
    /**
     * 获取：将数据库结果集转化为Java实例对象的关系的数量
     */
    public long getRelCount()
    {
        return relCount;
    }


    
    /**
     * 设置：将数据库结果集转化为Java实例对象的关系的数量
     * 
     * @param i_RelCount 将数据库结果集转化为Java实例对象的关系的数量
     */
    public void setRelCount(long i_RelCount)
    {
        this.relCount = i_RelCount;
    }
    
}
