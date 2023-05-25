package org.hy.common.xcql.junit;

import org.hy.common.Help;
import org.hy.common.xml.log.Logger;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;





/**
 * Neo4j的测试单元
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-05-24
 * @version     v1.0
 */
public class JU_Neo4j
{
    private static final Logger $Logger = new Logger(JU_Neo4j.class ,true);
    
    
    
    @Test
    public void test_Neo4j_001()
    {
        Driver        v_Driver        = GraphDatabase.driver("neo4j://127.0.0.1:7687" ,AuthTokens.basic("neo4j", "ZhengWei@qq.com"));
        SessionConfig v_SessionConfig = SessionConfig.forDatabase("cdc");
        Session       v_Session       = v_Driver.session(v_SessionConfig);
        
        Result v_Result = v_Session.run("MATCH (n:`数据源`) RETURN n");
        while (v_Result.hasNext())
        {
            Record v_Record = v_Result.next();
            
            Help.print(v_Record.keys());
            
            $Logger.info(v_Record.get("n").get("xid"));
        }
        v_Session.close();
        v_Driver.close();
    }
    
}
