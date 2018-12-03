package com.zhuoyan.es.config;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: wanhao
 * @Description
 * @Date: Created in 18:50 2018/11/29
 */
@Configuration
@ConditionalOnClass({HighRestESTemplete.class , RestHighLevelClient.class , RestClient.class})
@AutoConfigureAfter(ESAutoConfiguration.class)
public class ESDataAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(RestHighLevelClient.class)
    public HighRestESTemplete highRestESTemplete(RestHighLevelClient restHighLevelClient){
        try {
            return new HighRestESTemplete(restHighLevelClient);
        }catch (Exception e){
            throw new IllegalStateException(e);
        }
    }


}
