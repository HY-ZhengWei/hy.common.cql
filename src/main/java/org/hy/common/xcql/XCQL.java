package org.hy.common.xcql;

import java.util.HashMap;
import java.util.Map;

import org.hy.common.Busway;
import org.hy.common.CycleNextList;
import org.hy.common.TablePartitionBusway;
import org.hy.common.XJavaID;
import org.hy.common.xml.log.Logger;





/**
 * 解释Xml文件，执行占位符CQL，再分析数据库结果集转化为Java实例对象。
 * 
 * 1. 必须是数据库连接池的。但微型应用(如手机App)，可用无连接池概念的 org.hy.common.DataSourceNoPool 代替。
 * 
 * 2. 有占位符CQL分析功能。
 *    A. 可按对象填充占位符CQL; 同时支持动态CQL，动态标识 <[ ... ]>
 *    B. 可按集合填充占位符CQL。同时支持动态CQL，动态标识 <[ ... ]>
 *    C. CQL语句生成时，对于占位符，可实现xxx.yyy.www(或getXxx.getYyy.getWww)全路径的解释。如，':shool.BeginTime'
 * 
 * 3. 对结果集输出字段转为Java实例，有过滤功能。
 *    A. 可按字段名称过滤;
 *    B. 可按字段位置过滤。
 * 
 * 4. 支持XCQL触发器
 * 
 * 5.外界有自行定制的XCQL异常处理的机制
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public final class XCQL implements Comparable<XCQL> ,XJavaID
{
    private static final Logger                                $Logger         = new Logger(XCQL.class ,true);
    
    
    /** execute()方法中执行多条SQL语句的分割符 */
    public  static final String                                $Executes_Split = ";/";
    
    /** 每个XCQL对象的执行日志。默认每个XCQL对象只保留100条日志。按 getObjectID() 分区 */
    public  static final TablePartitionBusway<String ,XCQLLog> $CQLBuswayTP    = new TablePartitionBusway<String ,XCQLLog>();
    
    /** 所有CQL执行日志，有一定的执行顺序。默认只保留5000条执行过的CQL语句 */
    public  static final Busway<XCQLLog>                       $CQLBusway      = new Busway<XCQLLog>(5000);
    
    /** CQL执行异常的日志。默认只保留9000条执行异常的CQL语句 */
    public  static final Busway<XCQLLog>                       $CQLBuswayError = new Busway<XCQLLog>(9000);
    
    /** XCQL */
    public  static final String                                $XCQLErrors     = "XCQL-Errors";
    
    /**
     * 通用分区XSQL标示记录（确保只操作一次，而不是重复执行替换操作）
     * Map.key   为数据库类型 + "_" + XSQL.getObjectID()
     * Map.value 为 XSQL
     */
    private static final Map<String ,XCQL>                     $PagingMap      = new HashMap<String ,XCQL>();
                                                               
    /** 缓存大小 */
    protected static final int                                 $BufferSize     = 4 * 1024;
    
    
    
    static
    {
        $CQLBuswayTP.setDefaultWayLength(100);
        // XJava.putObject("$CQLBuswayTP"    ,$CQLBuswayTP);
        // XJava.putObject("$CQLBusway"      ,$CQLBusway);
        // XJava.putObject("$CQLBuswayError" ,$CQLBuswayError);
    }
    
    
    
    /** XJava池中对象的ID标识 */
    private String                         xjavaID;
    
    /**
     * 多个平行、平等的数据库的负载数据库集合
     * 
     * 实现多个平行、平等的数据库的负载均衡（简单级的）。
     * 目前建议只用在查询SQL上，当多个相同数据的数据库（如主备数据库），
     * 在高并发的情况下，提高整体查询速度，查询锁、查询阻塞等问题均能得到一定的解决。
     * 在高并发的情况下，突破数据库可分配的连接数量，会话数量将翻数倍（与数据库个数有正相关的关系
     */
    private CycleNextList<DataSourceCQL> dataSourceCQLs;
    
    /**
     * 数据库连接的域。
     * 
     * 它可与 this.dataSourceGroup 同时存在值，但 this.domain 的优先级高。
     * 当"域"存在时，使用域的数据库连接池组。其它情况，使用默认的数据库连接池组。
     */
    private XCQLDomain                     domain;
    
    /** 数据库占位符SQL的信息 */
    private DBCQL                          content;
    
    /** 解释Xml文件，分析数据库结果集转化为Java实例对象 */
    private XCQLResult                     result;
    
    
    
    @Override
    public void setXJavaID(String i_XJavaID)
    {
    }

    @Override
    public String getXJavaID()
    {
        return null;
    }

    @Override
    public void setComment(String i_Comment)
    {
        
    }

    @Override
    public String getComment()
    {
        return null;
    }

    @Override
    public int compareTo(XCQL o)
    {
        return 0;
    }
}
