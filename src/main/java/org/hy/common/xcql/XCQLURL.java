package org.hy.common.xcql;

import java.util.List;





/**
 * XCQL与访问URL的执行关系
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-02
 * @version     v1.0
 */
public class XCQLURL
{
    
    /** 访问的URL */
    private String       url;
    
    /** 访问的URL对应执行的CQL */
    private List<String> cqls;

    
    
    public XCQLURL()
    {
        
    }
    
    
    
    /**
     * 获取：访问的URL
     */
    public String getUrl()
    {
        return url;
    }
    

    
    /**
     * 设置：访问的URL
     * 
     * @param url
     */
    public void setUrl(String url)
    {
        this.url = url;
    }
    

    
    /**
     * 获取：访问的URL对应执行的CQL
     */
    public List<String> getCqls()
    {
        return cqls;
    }
    

    
    /**
     * 设置：访问的URL对应执行的CQL
     * 
     * @param i_CQLs
     */
    public void setCqls(List<String> i_CQLs)
    {
        this.cqls = i_CQLs;
    }
    
}
