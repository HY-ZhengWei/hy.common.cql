package org.hy.common.xcql;

import java.util.HashMap;
import java.util.Map;





/**
 * 数据库节点的元数据信息
 * 
 * Neo4j与关系数据库在这里不同的是：
 *    1. 生成元数据的执行时间和方式
 *       1.1 关系数据库：预先解释，解释一次处处使用
 *       1.2 Neo4j：每次都要解释，并且是在遍历所查所有数据后才能生成。
 *                  原因是Neo4j的每个节点属性均不一样
 *    2. 无数据类型
 *       2.1 Neo4j是Json格式的数据，没有明确的数据类型
 * 
 *    3. 列名无前后顺序
 *       3.1 Neo4j的每个节点属性均不一样
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public class DBTableMetaData
{
    
    /** Map.key = 格式化后的字段名称      Map.value = 原始的字段名称  */
    private Map<String  ,String>    col_ByName;
    
    /** 字段名称的样式 */
    private DBNameStyle             col_NameStyle;
    
    
    
    public DBTableMetaData(DBNameStyle i_ColNameStyle)
    {
        this.col_ByName    = new HashMap<String ,String>();
        this.col_NameStyle = i_ColNameStyle;
    }
    
    
    
    /**
     * 添加表的字段信息
     * 
     * @param i_ColName   字段的名称
     */
    public void addColumnInfo(String i_ColName)
    {
        String v_ColName = "";
        
        // ZhengWei(HY) Add 2015-10-09 可由外部控制字段名称的样式
        if ( this.col_NameStyle == DBNameStyle.$Upper )
        {
            v_ColName = i_ColName.trim().toUpperCase();
        }
        else if ( this.col_NameStyle == DBNameStyle.$Normal )
        {
            v_ColName = i_ColName.trim();
        }
        else
        {
            v_ColName = i_ColName.trim().toLowerCase();
        }
        
        this.col_ByName.put(v_ColName ,i_ColName);
    }
    
    
    
    /**
     * 返回字段个数
     * 
     * @return
     */
    public int getColumnSize()
    {
        return this.col_ByName.size();
    }
    
    
    
    /**
     * 清除集合数据
     */
    public void clear()
    {
        this.col_ByName.clear();
    }
    
    
    
    @Override
    public int hashCode()
    {
        return this.col_ByName.hashCode();
    }



    @Override
    public boolean equals(Object i_Other)
    {
        if ( i_Other == null )
        {
            return false;
        }
        else if ( i_Other instanceof DBTableMetaData )
        {
            return this.col_ByName.equals(((DBTableMetaData)i_Other).col_ByName);
        }
        else
        {
            return false;
        }
    }
    
}
