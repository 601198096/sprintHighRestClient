package com.zhuoyan.es.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @Author: wanhao
 * @Description high rest client配置信息
 * @Date: Created in 19:49 2018/11/27
 */
@ConfigurationProperties(prefix = "spring.rest.client.elasticsearch")
public class ESProperties {

    /**
     * description: es集群名称
     * remark:
     * */
    private String clusterName = "elasticsearch";

    /**
     * description: es节点，使用[',']隔开，端口使用9200,例[localhost:9200,192.168.1.1:9200]
     * remark:
     * */
    private String clusterNodes;

    /**
     * description: 协议(默认'http')
     * remark:
     * */
    private String scheme;

    /**
     * description: xpack用户名
     * remark:
     * */
    private String username;

    /**
     * description: xpack密码
     * remark:
     * */
    private String password;

    /**
     * description: 最大重试超时,默认30秒
     * remark:
     * */
    private Integer maxRetryTimeout;

    /**
     * description: 连接超时,默认1秒
     * remark:
     * */
    private Integer connectTimeout;

    /**
     * description: socket超时,默认30秒
     * remark:
     * */
    private Integer socketTimeout;

    /**
     * description: 线程数(默认Runtime.getRuntime().availableProcessors())
     * remark:
     * */
    private Integer threadCount;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getMaxRetryTimeout() {
        return maxRetryTimeout;
    }

    public void setMaxRetryTimeout(Integer maxRetryTimeout) {
        this.maxRetryTimeout = maxRetryTimeout;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public Integer getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(Integer threadCount) {
        this.threadCount = threadCount;
    }
}
