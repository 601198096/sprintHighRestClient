package com.zhuoyan.es.config;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: wanhao
 * @Description high rest client工厂
 * @Date: Created in 10:16 2018/11/28
 */
public class HighRestClientFactoryBean implements FactoryBean<RestHighLevelClient>, InitializingBean, DisposableBean {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private RestHighLevelClient restHighLevelClient;
    private ESProperties esProperties;

    public void setEsProperties(ESProperties esProperties) {
        this.esProperties = esProperties;
    }

    @Override
    public void destroy() throws Exception {
        try {
            log.info("Closing elasticSearch  client");
            if(null != restHighLevelClient){
                restHighLevelClient.close();
            }
        } catch (final Exception e) {
            log.error("Error closing ElasticSearch client: ", e);
        }
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public Class<?> getObjectType() {
        return RestHighLevelClient.class;
    }

    @Override
    public RestHighLevelClient getObject() throws Exception {
        return restHighLevelClient;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        createClient();
    }

    private void createClient() throws Exception {
        String clusterNodes = esProperties.getClusterNodes();

        RestClientBuilder restClientBuilder = RestClient.builder(this.getHttpHosts(clusterNodes));

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        //xpack账户认证
        if(StrUtil.isNotBlank(esProperties.getUsername()) && StrUtil.isNotBlank(esProperties.getPassword())){
            credentialsProvider.setCredentials(AuthScope.ANY , new UsernamePasswordCredentials(esProperties.getUsername() , esProperties.getPassword()));
        }

        //认证和线程数
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            if(null != esProperties.getThreadCount()){
                httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(esProperties.getThreadCount()).build());
            }
            return httpClientBuilder;
        });

        //超时超时设置
        restClientBuilder.setRequestConfigCallback(requestConfigCallback -> {
            if(null != esProperties.getConnectTimeout()){
                requestConfigCallback.setConnectTimeout(esProperties.getConnectTimeout());
            }
            if(null != esProperties.getSocketTimeout()){
                requestConfigCallback.setSocketTimeout(esProperties.getSocketTimeout());
            }
            return requestConfigCallback;
        });

        //重试时间
        if(null != esProperties.getMaxRetryTimeout()){
            restClientBuilder.setMaxRetryTimeoutMillis(esProperties.getMaxRetryTimeout());
        }
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);
    }

    /**
     * description: 创建分割nodes，创建httpHost数组
     * @param 
     * @return {@link HttpHost[]}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public HttpHost[] getHttpHosts(String clusterNodes){
        Assert.hasText(clusterNodes, "Cluster nodes source must not be null or empty!");

        //分割node节点
        String[] nodes = StrUtil.split(clusterNodes, StrUtil.COMMA);

        List<HttpHost> httpHosts = Arrays.stream(nodes).map(node -> {
            HttpHost httpHost;
            //分割host和端口
            String[] hostAndPort = StrUtil.split(node, StrUtil.COLON);

            Assert.isTrue(hostAndPort.length == 2,
                    () -> String.format("在[%s]集群节点中, node:[%s]存在错误 ! 格式必须是host:port!", clusterNodes, node));

            String host = hostAndPort[0].trim();
            String port = hostAndPort[1].trim();

            Assert.hasText(host, () -> String.format("在node:[%s]没找到host!", node));
            Assert.hasText(port, () -> String.format("在node:[%s]没找到port!", node));

            if (StrUtil.isNotBlank(esProperties.getScheme())) {
                httpHost = new HttpHost(host, Integer.parseInt(port), esProperties.getScheme());
            } else {
                httpHost = new HttpHost(host, Integer.parseInt(port));
            }
            return httpHost;
        }).collect(Collectors.toList());
        return ArrayUtil.toArray(httpHosts, HttpHost.class);
    }
}
