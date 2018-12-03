package com.zhuoyan.es.config;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @Author: wanhao
 * @Description 初始化读取信息
 * @Date: Created in 14:34 2018/6/11
 */
@Configuration
@ConditionalOnClass({RestHighLevelClient.class,HighRestClientFactoryBean.class, RestClient.class})
@ConditionalOnProperty(prefix = "spring.rest.client.elasticsearch" , name = "cluster-nodes")
@EnableConfigurationProperties(ESProperties.class)
public class ESAutoConfiguration {

    @Autowired
    private ESProperties esProperties;

    /**
     * description: 初始化自定义templete
     * @param
     * @return {@link RestHighLevelClient}
     * createdBy:wanhao
     * created:2018年07月04日
     * */
    @Bean
    @ConditionalOnMissingBean
    public RestHighLevelClient restHighLevelClient() throws Exception {
        HighRestClientFactoryBean highRestClientFactoryBean = new HighRestClientFactoryBean();
        highRestClientFactoryBean.setEsProperties(esProperties);
        highRestClientFactoryBean.afterPropertiesSet();
        return highRestClientFactoryBean.getObject();
    }
}
