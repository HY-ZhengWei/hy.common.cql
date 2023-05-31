package org.hy.common.xcql;

import org.hy.common.Help;
import org.hy.common.PartitionMap;
import org.hy.common.SplitSegment;
import org.hy.common.StringHelp;





/**
 * 分段CQL信息。
 * 
 * 通过 <[ ... ]> 分段的CQL
 * 
 * 同时解释占位符
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-05-25
 * @version     v1.0
 */
public class DBCQL_Split extends SplitSegment
{
    
    private static final long serialVersionUID = -7322548589241813089L;
    
    

    /**
     * 占位符信息的集合
     * 
     * placeholders属性为有降序排序顺序的TablePartitionLink。
     *   用于解决 #A、#AA 同时存在时的混乱。
     * 
     * Map.key    为占位符。前缀不包含#符号
     * Map.Value  为占位符的顺序。下标从0开始
     */
    private PartitionMap<String ,Integer> placeholders;
    
    /**
     * 占位符信息的集合（保持占位符原顺序不变）
     * 
     * Map.key    为占位符。前缀不包含#符号
     * Map.Value  为占位符的顺序。下标从0开始
     */
    private PartitionMap<String ,Integer> placeholdersSequence;
    
    
    
    public DBCQL_Split(SplitSegment i_SplitSegment)
    {
        super(i_SplitSegment);
    }
    
    
    
    /**
     * 占位符信息的集合
     * 
     * placeholders属性为有降序排序顺序的TablePartitionLink。
     *   用于解决 #A、#AA 同时存在时的混乱。
     * 
     * Map.key    为占位符。前缀不包含#符号
     * Map.Value  为占位符的顺序。下标从0开始
     */
    public PartitionMap<String ,Integer> getPlaceholders()
    {
        return placeholders;
    }
    
    
    
    /**
     * 占位符信息的集合（保持占位符原顺序不变）
     * 
     * Map.key    为占位符。前缀不包含#符号
     * Map.Value  为占位符的顺序。下标从0开始
     */
    public PartitionMap<String ,Integer> getPlaceholdersSequence()
    {
        return placeholdersSequence;
    }
    
    
    
    /**
     * 解释占位符
     */
    public synchronized void parsePlaceholders()
    {
        if ( Help.isNull(this.info) )
        {
            return;
        }
        
        this.placeholdersSequence = StringHelp.parsePlaceholdersSequence(DBCQL.$Placeholder ,this.info ,true);
        this.placeholders         = Help.toReverse(this.placeholdersSequence);
    }
    
    
    
    public int getPlaceholderSize()
    {
        if ( this.placeholders == null )
        {
            return 0;
        }
        else
        {
            return this.placeholders.size();
        }
    }
    
}
