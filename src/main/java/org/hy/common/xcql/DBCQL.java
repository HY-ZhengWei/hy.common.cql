package org.hy.common.xcql;

import java.io.Serializable;





/**
 * 图数据库占位符CQL的信息。
 * 
 * 主要对类似如下的CQL信息（我们叫它为:占位符CQL）进行分析后，并根据Java的 "属性类(或叫值对应类)" 转换为真实能被执行的CQL。
 * 
 *  MATCH (f:`源主键`),(t:`源端表` { tableName: '#tableName'})
 *  WHERE f.tableID = t.id
 *    AND f.orderBy = "#orderBy"
 *    AND NOT EXISTS { MATCH (f)-[:`主键`]-(t) }
 * CREATE (f)-[:主键]->(t)  ;
 * 
 * 原理是这样的：上述占位符CQL中的 '#tableName' 为占位符。将用 "属性类" getTableName() 方法的返回值进行替换操作。
 * 
 *            1. 当 "属性类" 没有对应的 getTableName() 方法时，
 *               生成的可执行CQL中将不包括 "BeginTime = TO_DATE(':BeginTime' ,'YYYY-MM-DD HH24:MI:SS')" 的部分。
 * 
 *            2. 当 "属性类" 有对应的 getBeginTime() 方法时，但返回值为 null时，
 *               生成的可执行SQL中将不包括 "BeginTime = TO_DATE(':BeginTime' ,'YYYY-MM-DD HH24:MI:SS')" 的部分。
 * 
 *            3. '#tableName' 占位符的命名，要符合Java的驼峰命名规则，但首字母可以大写，也可以小写。
 * 
 *            4. "#orderBy" 占位符对应 "属性类" getOrderBy() 方法的返回值类型为基础类型(int、double)时，
 *               不可能为 null 值的情况。即，此占位符在可执行CQL中是必须存在。
 *               如果想其可变，须使用 Integer、Double 类型的返回值类型。
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-05-24
 * @version     v1.0
 */
public class DBCQL implements Serializable
{

    private static final long serialVersionUID = -8245123127082501057L;
    
}
