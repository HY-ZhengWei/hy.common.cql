package org.hy.common.xcql;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hy.common.Date;
import org.hy.common.Help;
import org.hy.common.MethodReflect;
import org.hy.common.StringHelp;
import org.hy.common.xcql.event.DefaultXCQLResultFillEvent;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;





/**
 * 解释Xml文件，分析数据库结果集转化为Java实例对象
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public final class XCQLResult
{
    /**
     * 正则表达式对：row 关键字的识别 -- 表示行级对象
     * 如：add(row)
     */
    @SuppressWarnings("unused")
    private final static String $REGEX_ROW            = "[ \\(,][Rr][Oo][Ww][ \\),\\.]";
    
    /**
     * 正则表达式对：row.xxx 关键字的识别 -- 表示行级对象某一属性值的引用
     * 如：put(row.serialNo ,row)
     */
    @SuppressWarnings("unused")
    private final static String $REGEX_ROW_GETTER     = "[ \\(,][Rr][Oo][Ww]\\.\\w+[ \\),]";
    
    /**
     * 正则表达式对：rowNo 关键字的识别 -- 表示行号
     * 如：addColumnValue(rowNo ,colNo ,colValue)
     */
    @SuppressWarnings("unused")
    private final static String $REGEX_ROWNO          = "[ \\(,][Rr][Oo][Ww][Nn][Oo][ \\),]";
    
    /**
     * 正则表达式对：colNo 关键字的识别 -- 表示列号
     * 如：addColumnValue(rowNo ,colNo ,colValue)
     */
    @SuppressWarnings("unused")
    private final static String $REGEX_COLNAME        = "[ \\(,][Cc][Oo][Ll][Nn][Aa][Mm][Ee][ \\),]";
    
    /**
     * 正则表达式对：colNo 关键字的识别 -- 表示列号
     * 如：addColumnValue(rowNo ,colNo ,colValue)
     */
    @SuppressWarnings("unused")
    private final static String $REGEX_COLNO          = "[ \\(,][Cc][Oo][Ll][Nn][Oo][ \\),]";
    
    /**
     * 正则表达式对：colValue 关键字的识别 -- 表示某一列的数值
     * 如：addColumnValue(rowNo ,colNo ,colValue)
     */
    @SuppressWarnings("unused")
    private final static String $REGEX_COLVALUE       = "[ \\(,][Cc][Oo][Ll][Vv][Aa][Ll][Uu][Ee][ \\),]";
    
    /**
     * 正则表达式对：方法名称的识别
     * 如：add(row)
     */
    private final static String $REGEX_METHOD         = "\\w+[\\(]";
    
    /**
     * 正则表达式对：方法填写有效性的验证。
     * 如：xxx(p1 ,p2 ,... pn)
     * 如：xxx(o1.p1 ,o2.p1 ,... on.pn)
     */
    private final static String $REGEX_METHOD_VERIFY  = "^\\w+\\( *((\\w+\\.\\w+ *, *)|(\\w+ *, *))*((\\w+\\.\\w+)|(\\w+)) *\\)$";
    
    /**
     * 正则表达式对：特殊方法名称 setter 关键字的识别 -- 表示 setter 方法，使用反射机制向行级对象中 setter 属性
     * 如：setter(colValue)
     */
    @SuppressWarnings("unused")
    private final static String $REGEX_SETTER         = "[Ss][Ee][Tt][Tt][Ee][Rr][\\(]";
    
    
    
    /** 列级对象填充到行级对象中行级对象的方法类型：固定方法 */
    private final static int    $CFILL_METHOD_FIXED   = 0;
    
    /** 列级对象填充到行级对象中行级对象的方法类型：变化方法 */
    private final static int    $CFILL_METHOD_VARY    = 1;
    
    
    
    /** 表级对象的Class类型 */
    private Class<?>                table;
    
    /** 行级对象的Class类型 */
    private Class<?>                row;
    
    /** 行级对象填充到表级对象的填充方法字符串 */
    private String                  fill;
    
    /** 行级对象填充到表级对象的填充方法(解释 fill 后生成) */
    private XCQLMethod              fillMethod;
    
    /**
     * 行级对象填充到表级对象时，在填充之前触发的事件接口
     * 
     * 此事件接口，只允许有一个监听者，所以此变量的类型没有定义为集合。
     */
    private XCQLResultFillEvent     fillEvent;
    
    /** 列级对象填充到行级对象的填充方法字符串 */
    private String                  cfill;
    
    /**
     * 列级对象填充到行级对象中行级对象的方法类型(解释 cfill 后生成)
     * 
     * 1. 固定方法 -- 如：cfill = "addColumnValue(colNo ,colValue)"   -- $CFILL_METHOD_FIXED
     * 2. 变化方法 -- 如：cfill = "setter(colValue)"                  -- $CFILL_METHOD_VARY
     */
    private int                     cfillMethodType;
    
    /**
     * 列级对象填充到行级对象的填充方法(解释 cfill 后生成)
     * 
     * 当为固定方法时，cfillMethodArr只有一个元素
     * 当为变化方法时，cfillMethodArr的元素个数和顺序与CQL的输出结果相同
     * 
     * Map.key   Neo4j的属性名称，如 MATCH (n) RETURN n.id 时，Map.key 为 id ，即不带neo4j的别名"n."，并且区别大小写
     * Map.value 为setter 方法映射。当解释不当时为NULL
     */
    private Map<String ,XCQLMethod> cfillMethodArr;
    
    /** 字段名称的样式(默认为全部大写) */
    private DBNameStyle             cstyle;
    
    /** 结果集的元数据 */
    private DBTableMetaData         dbMetaData;
    
    /**
     * 标记出能表示一对多关系中归属同一对象的关系字段，组合关系的多个字段间用逗号分隔。
     * 
     * 关系字段一般为主表中的主键字段，或是主表存在于子表中的外键字段。
     * 
     * 此为可选项
     * 
     * ZhengWei(HY) Add 2017-03-01
     */
    private String                  relationKeys;
    
    
    /** 是否重新分析。任何一个对外的属性值变化后，都要重新分析。 */
    private boolean                 isAgainParse;
    
    
    
    public XCQLResult()
    {
        this.table           = ArrayList.class;
        this.row             = ArrayList.class;
        this.fill            = "add(row)";
        this.fillMethod      = new XCQLMethod();
        this.cfill           = "add(colValue)";
        this.cfillMethodType = $CFILL_METHOD_FIXED;
        this.cfillMethodArr  = null;
        this.cstyle          = DBNameStyle.$Upper;
        this.dbMetaData      = new DBTableMetaData(this.cstyle);
        this.relationKeys    = null;
        this.isAgainParse    = true;
    }
    
    
    
    /**
     * 将数据库结果集转化为Java实例对象
     * 
     * @param i_Result
     * @return
     */
    public XCQLData getDatas(Result i_Result)
    {
        return this.getDatas(i_Result ,0 ,0);
    }
    
    
    
    /**
     * 将数据库结果集转化为Java实例对象前，先解释元数据。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-03-19
     * @version     v1.0
     *
     * @param i_Result
     */
    private synchronized void getDatasParse(Result i_Result)
    {
        if ( this.isAgainParse )
        {
            this.parse();
        }
    }
    
    
    
    /**
     * 将数据库结果集转化为Java实例对象(私有的)
     * 
     * @param i_Result
     * @param i_StartRow         开始读取的行号。下标从0开始。
     * @param i_PagePerSize      每页显示多少条数据。只有大于0时，游标分页功能才生效。
     * @return
     */
    @SuppressWarnings("unchecked")
    private XCQLData getDatas(Result i_Result
                             ,int    i_StartRow
                             ,int    i_PagePerSize)
    {
        if ( i_Result == null )
        {
            throw new NullPointerException("Result is null.");
        }
        
        
        Object  v_Table         = null;
        long    v_RowNo         = 0;
        int     v_ColNo         = 0;
        String  v_FieldName     = "";
        boolean v_FillEvent     = false;
        Date    v_ExecBeginTime = null;
        
        try
        {
            v_Table = this.newTableObject();
            
            getDatasParse(i_Result);
            
            v_ExecBeginTime = new Date();
            
            // 游标分页功能。那怕是一丁点的性能，不性代码的冗余
            if ( i_PagePerSize > 0 )
            {
                int v_Count = 0;
                while ( v_Count < i_StartRow && i_Result.hasNext() )
                {
                    i_Result.next();
                    v_Count++;
                }
                v_Count = 0;
                
                // 不存在，行级对象填充到表级对象时的事件接口
                if ( null == fillEvent )
                {
                    // 列级对象填充到行级对象中行级对象的方法类型: 固定方法
                    if ( this.cfillMethodType == $CFILL_METHOD_FIXED )
                    {
                        XCQLMethod v_CFillMethod = this.cfillMethodArr.get(this.cfill);
                        
                        // 遍历每条记录
                        while ( v_Count < i_PagePerSize && i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    Object v_ColValue = v_RData.get(v_FieldName ,"");
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    Object v_ColValue = v_RData.asString();
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                            }
                            
                            this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                            v_Count++;
                        }
                    }
                    // 列级对象填充到行级对象中行级对象的方法类型: 变化方法 -- setter(colValue)
                    else
                    {
                        // 遍历每条记录
                        while ( v_Count < i_PagePerSize && i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                            }
                            
                            this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                            v_Count++;
                        }
                    }
                }
                // 外界用户定义了，行级对象填充到表级对象时的事件接口
                else
                {
                    Object v_RowPrevious = null;
                    
                    this.fillEvent.start(v_Table);
                    
                    // 列级对象填充到行级对象中行级对象的方法类型: 固定方法
                    if ( this.cfillMethodType == $CFILL_METHOD_FIXED )
                    {
                        XCQLMethod v_CFillMethod = this.cfillMethodArr.get(this.cfill);
                        
                        // 遍历每条记录
                        while ( v_Count < i_PagePerSize && i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    Object v_ColValue = v_RData.get(v_FieldName ,"");
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    Object v_ColValue = v_RData.asString();
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                            }
                            
                            v_FillEvent = true;
                            if ( this.fillEvent.before(v_Table ,v_Row ,v_RowNo ,v_RowPrevious) )
                            {
                                this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                                v_RowPrevious = v_Row;
                            }
                            v_FillEvent = false;
                            v_Count++;
                        }
                    }
                    // 列级对象填充到行级对象中行级对象的方法类型: 变化方法 -- setter(colValue)
                    else
                    {
                        // 遍历每条记录
                        while ( v_Count < i_PagePerSize && i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                            }
                            
                            v_FillEvent = true;
                            if ( this.fillEvent.before(v_Table ,v_Row ,v_RowNo ,v_RowPrevious) )
                            {
                                this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                                v_RowPrevious = v_Row;
                            }
                            v_FillEvent = false;
                            v_Count++;
                        }
                    }
                }
            }
            // 非游标分页功能。那怕是一丁点的性能，不性代码的冗余
            else
            {
                // 不存在，行级对象填充到表级对象时的事件接口
                if ( null == fillEvent )
                {
                    // 列级对象填充到行级对象中行级对象的方法类型: 固定方法
                    if ( this.cfillMethodType == $CFILL_METHOD_FIXED )
                    {
                        XCQLMethod v_CFillMethod = this.cfillMethodArr.get(this.cfill);
                        
                        // 遍历每条记录
                        while ( i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    Object v_ColValue = v_RData.get(v_FieldName ,"");
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    Object v_ColValue = v_RData.asString();
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                            }
                            
                            this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                        }
                    }
                    // 列级对象填充到行级对象中行级对象的方法类型: 变化方法 -- setter(colValue)
                    else
                    {
                        // 遍历每条记录
                        while ( i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                            }
                            
                            this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                        }
                    }
                }
                // 外界用户定义了，行级对象填充到表级对象时的事件接口
                else
                {
                    Object v_RowPrevious = null;
                    
                    this.fillEvent.start(v_Table);
                    
                    // 列级对象填充到行级对象中行级对象的方法类型: 固定方法
                    if ( this.cfillMethodType == $CFILL_METHOD_FIXED )
                    {
                        XCQLMethod v_CFillMethod = this.cfillMethodArr.get(this.cfill);
                        
                        // 遍历每条记录
                        while ( i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    Object v_ColValue = v_RData.get(v_FieldName ,"");
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    Object v_ColValue = v_RData.asString();
                                    v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                }
                            }
                            
                            v_FillEvent = true;
                            if ( this.fillEvent.before(v_Table ,v_Row ,v_RowNo ,v_RowPrevious) )
                            {
                                this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                                v_RowPrevious = v_Row;
                            }
                            v_FillEvent = false;
                        }
                    }
                    // 列级对象填充到行级对象中行级对象的方法类型: 变化方法 -- setter(colValue)
                    else
                    {
                        // 遍历每条记录
                        while ( i_Result.hasNext() )
                        {
                            Object       v_Row    = this.newRowObject();
                            Record       v_Record = i_Result.next();
                            List<String> v_RNames = v_Record.keys();
                            
                            // 遍历每条记录中的每个数据子集
                            // 识别类似 MATCH (n1:XX) ,(n2:YY) RETURN n1 ,n2 中的 RETURN 的数据子集 n1 和 n2
                            for (String v_RName : v_RNames)
                            {
                                Value            v_RData      = v_Record.get(v_RName);
                                Iterable<String> v_FieldNames = v_RData.keys();
                                boolean          v_IsEmpty    = true;
                                
                                // 遍历节点属性
                                // 识别类似 MATCH (n) RETURN n 中的 n 的属性
                                for (String v_FName : v_FieldNames)
                                {
                                    v_FieldName = v_FName;
                                    v_IsEmpty   = false;
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                                
                                // 处理非数据子集的，具体指定RETURN的属性
                                // 识别类似 MATCH (n) RETURN n.id ,n.name AS userName 中 n.id 和 userName
                                if ( v_IsEmpty )
                                {
                                    String [] v_FieldNameArr = v_RName.split("\\.");
                                    
                                    v_FieldName = v_FieldNameArr[0];
                                    if ( v_FieldNameArr.length >= 2 )
                                    {
                                        v_FieldName = v_FieldNameArr[1];
                                    }
                                    
                                    XCQLMethod v_CFillMethod = this.parseCFill(v_FieldName);
                                    if ( v_CFillMethod != null )
                                    {
                                        Object v_ColValue = v_CFillMethod.getResultSet_Getter().invoke(v_RData ,v_FieldName ,null);
                                        v_ColValue = v_CFillMethod.getMachiningValue().getValue(v_ColValue);
                                        v_CFillMethod.invoke(v_Row ,v_ColValue ,(Long)null ,v_FieldName);
                                    }
                                }
                            }
                            
                            v_FillEvent = true;
                            if ( this.fillEvent.before(v_Table ,v_Row ,v_RowNo ,v_RowPrevious) )
                            {
                                this.fillMethod.invoke(v_Table ,v_Row ,v_RowNo++ ,null);
                                v_RowPrevious = v_Row;
                            }
                            v_FillEvent = false;
                        }
                    }
                }
            }
            
        }
        catch (Exception exce)
        {
            if ( !v_FillEvent )
            {
                throw new java.lang.RuntimeException("RowNo=" + v_RowNo + "  ColNo=" + v_ColNo + "  ColName=" + v_FieldName + "  " + exce.getMessage());
            }
            else
            {
                throw new java.lang.RuntimeException("Call FillEvent Error for RowNo=" + v_RowNo + "  " + exce.getMessage());
            }
        }
        
        return new XCQLData(v_Table ,v_RowNo ,this.cfillMethodArr.size() ,Date.getNowTime().differ(v_ExecBeginTime) ,this.dbMetaData);
    }
    
    
    
    /**
     * 全量解释。
     * 
     * 可手动调用。也可被自动调用。
     * 
     * 即，此方法不用人为刻意的被调用。
     */
    public synchronized void parse()
    {
        // 防止被重复解释
        if ( !this.isAgainParse )
        {
            return;
        }
        
        
        if ( this.table == null )
        {
            throw new NullPointerException("Table is null.");
        }
        
        if ( this.row == null )
        {
            throw new NullPointerException("Row is null.");
        }
        
        if ( Help.isNull(this.fill) )
        {
            throw new NullPointerException("Fill is null.");
        }
        
        if ( Help.isNull(this.cfill) )
        {
            throw new NullPointerException("CFill is null.");
        }
        
        
        this.dbMetaData.clear();
        this.fillMethod.clear();
        this.cfillMethodArr = null;
        
        
        this.parseFill();
        this.parseCFill();
        this.parseRelationKeys();
        
        
        this.isAgainParse = false;
    }
    
    
    
    /**
     * 解释 this.fill -- 行级对象填充到表级对象的填充方法字符串
     */
    private void parseFill()
    {
        Pattern      v_Pattern    = Pattern.compile($REGEX_METHOD);
        Matcher      v_Matcher    = v_Pattern.matcher(this.fill);
        String       v_MethodName = "";
        int          v_EndIndex   = 0;
        List<Method> v_MethodList = null;
        String       v_Params     = null;
        String []    v_ParamArr   = null;
        
        
        // 识别行级填充方法名称
        if ( v_Matcher.find() )
        {
            v_MethodName = v_Matcher.group();
            v_MethodName = v_MethodName.substring(0 ,v_MethodName.length() - 1);
            v_EndIndex   = v_Matcher.end();
        }
        else
        {
            throw new RuntimeException("Fill method name[" + this.fill + "] is not exist.");
        }
        
        
        v_Params   = this.fill.substring(v_EndIndex ,this.fill.length() - 1);
        v_ParamArr = v_Params.split(",");
        
        Class<?> [] v_ParamClassArr_int     = new Class[v_ParamArr.length];
        Class<?> [] v_ParamClassArr_Integer = new Class[v_ParamArr.length];
        
        
        // 识别行级填充方法的所有入参
        for (int i=0; i<v_ParamArr.length; i++)
        {
            XCQLMethodParam v_MethodParam = null;
            
            if ( "ROW".equalsIgnoreCase(v_ParamArr[i].trim()) )
            {
                v_MethodParam = XCQLMethodParam_Fill.getInstance(XCQLMethodParam_Fill.$FILL_ROW );
                
                v_ParamClassArr_int[i]     = Object.class;
                v_ParamClassArr_Integer[i] = Object.class;
            }
            else if ( "ROWNO".equalsIgnoreCase(v_ParamArr[i].trim()) )
            {
                v_MethodParam = XCQLMethodParam_Fill.getInstance(XCQLMethodParam_Fill.$FILL_ROW_NO);
                
                v_ParamClassArr_int[i]     = int.class;
                v_ParamClassArr_Integer[i] = Integer.class;
            }
            else if ( "ROW.".equalsIgnoreCase(v_ParamArr[i].trim().substring(0 ,4)) )
            {
                // 识别对象属性的方法名
                String       v_AttrMethodName = "get" + v_ParamArr[i].trim().substring(4);
                List<Method> v_AttrMethodList = MethodReflect.getMethodsIgnoreCase(this.row ,v_AttrMethodName ,0);
                
                if ( v_AttrMethodList.size() == 0 )
                {
                    throw new RuntimeException("Row.Getter method name[" + v_ParamArr[i].trim() + "] is not exist.");
                }
                else if ( v_AttrMethodList.size() > 1 )
                {
                    // 对象属性方法有多个重载方法，无法正确识别
                    throw new RuntimeException("Row.Getter method name[" + v_ParamArr[i].trim() + "] have much override methods.");
                }
                
                v_ParamClassArr_int[i]     = v_AttrMethodList.get(0).getReturnType();
                v_ParamClassArr_Integer[i] = v_AttrMethodList.get(0).getReturnType();
                
                v_MethodParam = XCQLMethodParam_Fill.getInstance(XCQLMethodParam_Fill.$FILL_ROW_GETTER ,v_AttrMethodList.get(0));
            }
            else
            {
                // 方法的参数形式只能是 row、rowNo、row.xxx
                throw new RuntimeException("Fill method[" + this.fill + "] Parameter is not valid. Parameter only 'row' or 'rowNo' or 'row.xxx'.");
            }
            
            this.fillMethod.addParam(v_MethodParam);
        }
        
        
        // 获取行级填充方法
        v_MethodList = MethodReflect.getMethodsIgnoreCase(this.table ,v_MethodName ,v_ParamArr.length);
        if ( v_MethodList.size() == 1 )
        {
            this.fillMethod.setCall(v_MethodList.get(0));
        }
        else if ( v_MethodList.size() > 1 )
        {
            for (int v_Override=0; v_Override<v_MethodList.size() && this.fillMethod.getCall() == null; v_Override++)
            {
                Class<?> [] v_ClassArr = v_MethodList.get(v_Override).getParameterTypes();
                
                if ( this.equalsMethodParamTypes(v_ClassArr ,v_ParamClassArr_int) )
                {
                    this.fillMethod.setCall(v_MethodList.get(v_Override));
                }
                else if ( this.equalsMethodParamTypes(v_ClassArr ,v_ParamClassArr_Integer) )
                {
                    this.fillMethod.setCall(v_MethodList.get(v_Override));
                }
            }
            
            // 行级填充方法有多个重载方法，无法正确识别
            if ( this.fillMethod.getCall() == null )
            {
                throw new RuntimeException("Fill method name[" + this.fill + "] have much override methods.");
            }
        }
        else
        {
            throw new RuntimeException("Fill method name[" + this.fill + "] is not exist.");
        }
        
    }
    
    
    
    /**
     * 解释 this.cfill -- 列级对象填充到行级对象的填充方法字符串
     * 
     * 本方法仅适用于 Setter 的"变化方法"的解释。
     * 
     * 本方法是一个属性一个属性的解释，因为图数据库是非结构化的
     * 
     * 本方法是运行时的动态解释
     *    一边获取图数据库中的数据，边解释，
     *    并不是关系数据库的预先解释好，再获取数据库中的数据。
     * 
     * @author      ZhengWei(HY)
     * @createDate  2023-06-03
     * @version     v1.0
     *
     * @param i_Neo4jFieldName   Neo4j的属性名称，如 MATCH (n) RETURN n.id 时，为 id ，即不带neo4j的别名"n."，并且区别大小写
     */
    private XCQLMethod parseCFill(String i_Neo4jFieldName)
    {
        if ( this.cfillMethodType != $CFILL_METHOD_VARY )
        {
            return null;
        }
        else if ( this.cfillMethodArr.containsKey(i_Neo4jFieldName) )
        {
            // 防止重复解释：非同步锁的
            return this.cfillMethodArr.get(i_Neo4jFieldName);
        }
        
        synchronized (this)
        {
            if ( this.cfillMethodArr.containsKey(i_Neo4jFieldName) )
            {
                // 防止重复解释：同步锁内的
                return this.cfillMethodArr.get(i_Neo4jFieldName);
            }
            
            List<Method> v_MethodList        = null;
            String       v_ColName           = i_Neo4jFieldName;
            String []    v_ColNameArr        = v_ColName.split("\\.");
            Method       v_InnerObjGetMethod = null;
            Method       v_InnerObjSetMethod = null;
            Class<?>     v_Collection        = null;
            Class<?>     v_CollectionElement = null;
            
            if ( v_ColNameArr.length >= 2 )
            {
                // 对象A的属性还是一个对象B，现对对象B的属性进行填充 ZhengWei(HY) Add 2015-07-04
                v_InnerObjGetMethod = MethodReflect.getGetMethod(this.row ,v_ColNameArr[0] ,true);
                
                if ( MethodReflect.isExtendImplement(v_InnerObjGetMethod.getReturnType() ,List.class) )
                {
                    // 支持对象B为集合的情况。A与B为：一对多关系。 ZhengWei(HY) Add 2017-03-01
                    v_Collection        = List.class;
                    v_CollectionElement = MethodReflect.getGenericsReturn(v_InnerObjGetMethod).getGenericType();
                    v_MethodList        = MethodReflect.getMethodsIgnoreCase(v_CollectionElement ,"set" + v_ColNameArr[1] ,1);
                }
                else if ( MethodReflect.isExtendImplement(v_InnerObjGetMethod.getReturnType() ,Set.class) )
                {
                    // 支持对象B为集合的情况。A与B为：一对多关系。 ZhengWei(HY) Add 2017-03-01
                    v_Collection        = Set.class;
                    v_CollectionElement = MethodReflect.getGenericsReturn(v_InnerObjGetMethod).getGenericType();
                    v_MethodList        = MethodReflect.getMethodsIgnoreCase(v_CollectionElement ,"set" + v_ColNameArr[1] ,1);
                }
                else
                {
                    v_MethodList = MethodReflect.getMethodsIgnoreCase(v_InnerObjGetMethod.getReturnType() ,"set" + v_ColNameArr[1] ,1);
                }
                
                try
                {
                    v_InnerObjSetMethod = this.row.getMethod("s" + v_InnerObjGetMethod.getName().substring(1) ,v_InnerObjGetMethod.getReturnType());
                }
                catch (Exception exce)
                {
                    // Nothing. 没有对应的 "set对象B(...)"的方法。
                    // 没有就没有吧，只要使用方保证 "get对象B()" 的方法的返回值不为空就成。
                }
            }
            else
            {
                v_MethodList = MethodReflect.getMethodsIgnoreCase(this.row ,"set" + v_ColName ,1);
            }
            
            XCQLMethod v_XCQLMethod = null;
            if ( v_MethodList.size() >= 1 )
            {
                v_XCQLMethod = new XCQLMethod();
                v_XCQLMethod.setCall(v_MethodList.get(0));
                v_XCQLMethod.addParam(XCQLMethodParam_CFill.getInstance(XCQLMethodParam_CFill.$CFILL_COL_VALUE));
                v_XCQLMethod.setGetInstanceOfMethod(v_InnerObjGetMethod);
                v_XCQLMethod.setSetInstanceOfMethod(v_InnerObjSetMethod);
                v_XCQLMethod.setCollection(         v_Collection);
                v_XCQLMethod.setCollectionElement(  v_CollectionElement);
                
                // 按 call 方法的入参类型，决定 org.neo4j.driver.Result 获取字段值的方法
                v_XCQLMethod.parseResultSet_Getter();
            }
            this.dbMetaData.addColumnInfo(i_Neo4jFieldName);
            this.cfillMethodArr.put(i_Neo4jFieldName ,v_XCQLMethod);
            
            return v_XCQLMethod;
        }
    }
    
    
    
    /**
     * 解释 this.cfill -- 列级对象填充到行级对象的填充方法字符串
     */
    private void parseCFill()
    {
        Pattern      v_Pattern    = Pattern.compile($REGEX_METHOD);
        Matcher      v_Matcher    = v_Pattern.matcher(this.cfill);
        String       v_MethodName = "";
        int          v_EndIndex   = 0;
        List<Method> v_MethodList = null;
        String       v_Params     = null;
        String []    v_ParamArr   = null;
        
        
        // 识别列级填充方法名称
        if ( v_Matcher.find() )
        {
            v_MethodName = v_Matcher.group();
            v_MethodName = v_MethodName.substring(0 ,v_MethodName.length() - 1);
            v_EndIndex   = v_Matcher.end();
        }
        else
        {
            throw new RuntimeException("CFill method name[" + this.cfill + "] is not exist.");
        }
        
        
        this.cfillMethodArr = new LinkedHashMap<String ,XCQLMethod>();
        if ( "SETTER".equalsIgnoreCase(v_MethodName) )
        {
            this.cfillMethodType = $CFILL_METHOD_VARY;
            // 通过 parseCFill(String i_Neo4jFieldName) 运行时动态解释
        }
        else
        {
            this.cfillMethodType = $CFILL_METHOD_FIXED;
            
            v_Params   = this.cfill.substring(v_EndIndex ,this.cfill.length() - 1);
            v_ParamArr = v_Params.split(",");
            
            Class<?> [] v_ParamClassArr_int     = new Class[v_ParamArr.length];
            Class<?> [] v_ParamClassArr_Integer = new Class[v_ParamArr.length];
            
            
            // 识别列级填充方法的所有入参
            for (int i=0; i<v_ParamArr.length; i++)
            {
                XCQLMethodParam v_MethodParam = null;
                
                if ( "COLVALUE".equalsIgnoreCase(v_ParamArr[i].trim()) )
                {
                    v_MethodParam = XCQLMethodParam_CFill.getInstance(XCQLMethodParam_CFill.$CFILL_COL_VALUE);
                    
                    v_ParamClassArr_int    [i] = Object.class;
                    v_ParamClassArr_Integer[i] = Object.class;
                }
                else if ( "COLNAME".equalsIgnoreCase(v_ParamArr[i].trim()) )
                {
                    v_MethodParam = XCQLMethodParam_CFill.getInstance(XCQLMethodParam_CFill.$CFILL_COL_NAME);
                    
                    v_ParamClassArr_int    [i] = String.class;
                    v_ParamClassArr_Integer[i] = String.class;
                }
                else
                {
                    // 方法的参数形式只能是 colValue、colNo、colName
                    throw new RuntimeException("CFill method[" + this.cfill + "] Parameter is not valid. Parameter only 'colValue' or 'colNo' or 'colName'.");
                }
                
                XCQLMethod v_XCQLMethod = new XCQLMethod();
                v_XCQLMethod.addParam(v_MethodParam);
                this.cfillMethodArr.put(this.cfill ,v_XCQLMethod);
            }
            
            
            // 获取列级填充方法
            v_MethodList = MethodReflect.getMethodsIgnoreCase(this.row ,v_MethodName ,v_ParamArr.length);
            if ( v_MethodList.size() == 1 )
            {
                this.cfillMethodArr.get(this.cfill).setCall(v_MethodList.get(0));
            }
            else if ( v_MethodList.size() > 1 )
            {
                for (int v_Override=0; v_Override<v_MethodList.size() && this.cfillMethodArr.get(this.cfill).getCall() == null; v_Override++)
                {
                    Class<?> [] v_ClassArr = v_MethodList.get(v_Override).getParameterTypes();
                    
                    if ( this.equalsMethodParamTypes(v_ClassArr ,v_ParamClassArr_int) )
                    {
                        this.cfillMethodArr.get(this.cfill).setCall(v_MethodList.get(v_Override));
                    }
                    else if ( this.equalsMethodParamTypes(v_ClassArr ,v_ParamClassArr_Integer) )
                    {
                        this.cfillMethodArr.get(this.cfill).setCall(v_MethodList.get(v_Override));
                    }
                }
                
                // 列级填充方法有多个重载方法，无法正确识别
                if ( this.cfillMethodArr.get(this.cfill).getCall() == null )
                {
                    throw new RuntimeException("CFill method name[" + this.cfill + "] have much override methods.");
                }
            }
            else
            {
                throw new RuntimeException("CFill method name[" + this.cfill + "] is not exist.");
            }
        }
        
    }
    
    
    
    /**
     * 解释：一对多关系时，识别出属于同一对象的主键信息（多个属性间用逗号分隔）
     * 
     * @author      ZhengWei(HY)
     * @createDate  2017-03-01
     * @version     v1.0
     *
     */
    private void parseRelationKeys()
    {
        if ( Help.isNull(this.relationKeys) || this.row == null )
        {
            return;
        }
        
        List<Method>        v_RelationKeyMethods   = new ArrayList<Method>();
        Map<Method ,Object> v_RelationValueMethods = new HashMap<Method ,Object>();
        
        String [] v_Keys = StringHelp.replaceAll(this.relationKeys ,new String[]{" " ,"\t" ,"\r" ,"\n"} ,new String[]{""}).split(",");
        for (int v_Index=0; v_Index<v_Keys.length; v_Index++)
        {
            Method v_Method = MethodReflect.getGetMethod(this.row ,v_Keys[0] ,true);
            
            if ( v_Method != null )
            {
                v_RelationKeyMethods.add(v_Method);
            }
        }
        
        for (XCQLMethod v_XCQLMethod : this.cfillMethodArr.values())
        {
            if ( v_XCQLMethod != null && v_XCQLMethod.getGetInstanceOfMethod() != null )
            {
                if ( MethodReflect.isExtendImplement(v_XCQLMethod.getGetInstanceOfMethod().getReturnType() ,Collection.class) )
                {
                    v_RelationValueMethods.put(v_XCQLMethod.getGetInstanceOfMethod() ,v_XCQLMethod.getGetInstanceOfMethod());
                }
            }
        }
        
        if ( !Help.isNull(v_RelationKeyMethods) && !Help.isNull(v_RelationValueMethods) )
        {
            this.fillEvent = new DefaultXCQLResultFillEvent(v_RelationKeyMethods ,Help.toListKeys(v_RelationValueMethods));
        }
        else
        {
            this.fillEvent = null;
        }
    }
    
    
    
    /**
     * 对比方法入参类型是否完全相等。
     * 
     * 对参数类为 Object.class 做相等处理。
     * 
     * @param i_ClassArr_01
     * @param i_ClassArr_02
     * @return
     */
    private boolean equalsMethodParamTypes(Class<?> [] i_ClassArr_01 ,Class<?> [] i_ClassArr_02)
    {
        if ( i_ClassArr_01.length != i_ClassArr_02.length )
        {
            return false;
        }
        
        
        for (int i=0; i<i_ClassArr_01.length; i++)
        {
            if ( i_ClassArr_01[i] == Object.class || i_ClassArr_02[i] == Object.class )
            {
                // Nothing.
            }
            else if ( i_ClassArr_01[i] != i_ClassArr_02[i] )
            {
                return false;
            }
        }
        
        
        return true;
    }
    
    
    
    /**
     * 方法填写有效性的验证
     * 
     * 如：xxx(p1 ,p2 ,... pn)
     * 如：xxx(o1.p1 ,o2.p1 ,... on.pn)
     * 
     * @param i_Text
     * @return
     */
    private boolean methodVerify(String i_Text)
    {
        Pattern v_Pattern = Pattern.compile($REGEX_METHOD_VERIFY);
        Matcher v_Matcher = v_Pattern.matcher(i_Text);
        
        return v_Matcher.find();
    }
    
    
    
    /**
     * 实例化一个表级对象
     * 
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     */
    private Object newTableObject() throws ClassNotFoundException, InstantiationException
    {
        Object v_TableInstance = null;
        
        try
        {
            v_TableInstance = this.table.newInstance();
        }
        catch (Exception exce)
        {
            throw new InstantiationException("Table Class(" + this.table + ") instantiation is error.");
        }
        
        return v_TableInstance;
    }
    
    
    
    /**
     * 实例化一个行级对象
     * 
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     */
    private Object newRowObject() throws ClassNotFoundException, InstantiationException
    {
        Object v_TableInstance = null;
        
        try
        {
            v_TableInstance = this.row.newInstance();
        }
        catch (Exception exce)
        {
            throw new InstantiationException("Row Class(" + this.row + ") instantiation is error.");
        }
        
        return v_TableInstance;
    }
    
    
    
    /**
     * 获取 ClassName 全路径的 Class 类型
     * 
     * @param i_ClassURL
     * @return
     */
    private Class<?> getClass(String i_ClassURL)
    {
        Class<?> v_Class = null;
        
        try
        {
            v_Class = Help.forName(i_ClassURL);
        }
        catch (Exception exce)
        {
            return null;
        }
        
        return v_Class;
    }
    
    
    
    public String getCfill()
    {
        return cfill;
    }
    
    

    public void setCfill(String i_CFill)
    {
        if ( Help.isNull(i_CFill) )
        {
            throw new NullPointerException("CFill is null.");
        }
        
        if ( this.methodVerify(i_CFill.trim()) )
        {
            if ( i_CFill.trim().toUpperCase(Locale.ENGLISH).startsWith("SETTER(") )
            {
                if ( !"SETTER(COLVALUE)".equalsIgnoreCase(i_CFill.trim()) )
                {
                    throw new RuntimeException("CFill[" + i_CFill + "] inconformity standard. Setter method parameter Only 'colValue'.");
                }
            }
            
            this.cfill        = i_CFill.trim();
            this.isAgainParse = true;
        }
        else
        {
            // 填充方法不符合规范
            throw new RuntimeException("CFill[" + i_CFill + "] inconformity standard.");
        }
    }
    
    

    public String getFill()
    {
        return fill;
    }

    
    
    public void setFill(String i_Fill)
    {
        if ( Help.isNull(i_Fill) )
        {
            throw new NullPointerException("Fill is null.");
        }
        
        if ( this.methodVerify(i_Fill.trim()) )
        {
            this.fill         = i_Fill.trim();
            this.isAgainParse = true;
        }
        else
        {
            // 填充方法不符合规范
            throw new RuntimeException("Fill[" + i_Fill + "] inconformity standard");
        }
    }
    

    
    /**
     * 获取：行级对象填充到表级对象时，在填充之前触发的事件接口
     * 
     * 此事件接口，只允许有一个监听者，所以此变量的类型没有定义为集合。
     */
    public XCQLResultFillEvent getFillEvent()
    {
        return fillEvent;
    }


    
    /**
     * 设置：行级对象填充到表级对象时，在填充之前触发的事件接口
     * 
     * 此事件接口，只允许有一个监听者，所以此变量的类型没有定义为集合。
     * 
     * @param fillEvent
     */
    public void setFillEvent(XCQLResultFillEvent fillEvent)
    {
        this.fillEvent = fillEvent;
    }



    /**
     * 行级对象的Class类型
     * 
     * @return
     */
    public Class<?> getRow()
    {
        return this.row;
    }
    
    
    
    /**
     * 行级对象的Class类型
     * 
     * @param i_Row
     * @throws ClassNotFoundException
     */
    public void setRow(String i_Row) throws ClassNotFoundException
    {
        if ( Help.isNull(i_Row) )
        {
            throw new NullPointerException("Row is null.");
        }
        
        
        Class<?> v_Class = this.getClass(i_Row.trim());
        
        if ( v_Class != null )
        {
            if ( v_Class.isInterface() )
            {
                throw new ClassCastException("Row Class[" + i_Row + "] is Interface ,but it is not new Instance.");
            }
            
            this.row          = v_Class;
            this.isAgainParse = true;
        }
        else
        {
            throw new ClassNotFoundException("Row Class[" + i_Row + "] is not exist.");
        }
    }
    
    
    
    /**
     * 表级对象的Class类型
     * 
     * @return
     */
    public Class<?> getTable()
    {
        return this.table;
    }
    
    
    
    /**
     * 表级对象的Class类型
     * 
     * @param i_Table
     * @throws ClassNotFoundException
     */
    public void setTable(String i_Table) throws ClassNotFoundException
    {
        if ( Help.isNull(i_Table) )
        {
            throw new NullPointerException("Table is null.");
        }
        
        
        Class<?> v_Class = this.getClass(i_Table.trim());
        
        if ( v_Class != null )
        {
            if ( v_Class.isInterface() )
            {
                throw new ClassCastException("Table Class[" + i_Table + "] is Interface ,but it is not new Instance.");
            }
            
            this.table        = v_Class;
            this.isAgainParse = true;
        }
        else
        {
            throw new ClassNotFoundException("Table Class[" + i_Table + "] is not exist.");
        }
    }
    
    
    
    /**
     * 获取：字段名称的样式(默认为全部大写)
     */
    public DBNameStyle getCstyle()
    {
        return this.cstyle;
    }
    
    
    
    /**
     * 设置：字段名称的样式(默认为全部大写)
     * 
     * @param cstyle
     */
    public void setCstyle(String i_CStyleName)
    {
        this.cstyle       = DBNameStyle.get(i_CStyleName);
        this.dbMetaData   = new DBTableMetaData(this.cstyle);
        this.isAgainParse = true;
    }
    

    
    /**
     * 标记出能表示一对多关系中归属同一对象的关系字段，组合关系的多个字段间用逗号分隔。
     * 
     * 关系字段一般为主表中的主键字段，或是主表存在于子表中的外键字段。
     * 
     * 此为可选项
     * 
     * ZhengWei(HY) Add 2017-03-01
     */
    public String getRelationKeys()
    {
        return this.relationKeys;
    }


    
    /**
     * 标记出能表示一对多关系中归属同一对象的关系字段，组合关系的多个字段间用逗号分隔。
     * 
     * 关系字段一般为主表中的主键字段，或是主表存在于子表中的外键字段。
     * 
     * 此为可选项
     * 
     * ZhengWei(HY) Add 2017-03-01
     * 
     * @param i_RelationKeys
     */
    public void setRelationKeys(String i_RelationKeys)
    {
        this.relationKeys = i_RelationKeys;
        this.isAgainParse = true;
    }


    
    /**
     * 是否分析过？
     * 或是，是否需要重新分析？
     * 
     * @return
     */
    public boolean isParsed()
    {
        return !this.isAgainParse;
    }
    
    
    
    /**
     * 获取查询结果集的字段结构
     * 
     * @author      ZhengWei(HY)
     * @createDate  2022-10-06
     * @version     v1.0
     *
     * @return
     */
    public DBTableMetaData getDBTableMetaData()
    {
        return this.dbMetaData;
    }
    
}
