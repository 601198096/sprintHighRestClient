package com.zhuoyan.es.config;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.zhuoyan.es.util.Docs;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.data.domain.Page;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: wanhao
 * @Description high rest client基础工具类
 * @Date: Created in 15:52 2018/11/28
 */
public class HighRestESTemplete{

    private RestHighLevelClient restHighLevelClient;

    public HighRestESTemplete(RestHighLevelClient restHighLevelClient) {

        Assert.notNull(restHighLevelClient, "Client must not be null!");

        this.restHighLevelClient = restHighLevelClient;
    }

    public RestHighLevelClient getClient(){
        return restHighLevelClient;
    }

    /**
     * description: 单纯的创建索引
     * @param: 
    * @param indexName
     * @return {@link boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public boolean createIndex(String indexName) throws IOException{
        return restHighLevelClient.indices().create(Requests.createIndexRequest(indexName)).isAcknowledged();
    }

    /**
     * description: 创建索引,并且可以设置settings,mapping,alias
     * @param:
    * @param indexName
    * @param type
    * @param settings
    * @param mappings
    * @param alias 别名
     * @return {@link boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public boolean createIndex(String indexName, String type , Object settings , Object mappings , String alias) throws IOException{
        CreateIndexRequest indexRequest = Requests.createIndexRequest(indexName);
        if(null != settings){
            if (settings instanceof String) {
                indexRequest.settings(String.valueOf(settings), Requests.INDEX_CONTENT_TYPE);
            } else if (settings instanceof Map) {
                indexRequest.settings((Map) settings);
            } else if (settings instanceof XContentBuilder) {
                indexRequest.settings((XContentBuilder) settings);
            } else if(settings instanceof Settings.Builder){
                indexRequest.settings((Settings.Builder)settings);
            }
        }
        if(StrUtil.isNotBlank(type) && null != mappings){
            if (mappings instanceof String) {
                indexRequest.mapping(type , String.valueOf(mappings), XContentType.JSON);
            } else if (mappings instanceof Map) {
                indexRequest.mapping(type , (Map) mappings);
            } else if (mappings instanceof XContentBuilder) {
                indexRequest.mapping(type , (XContentBuilder) mappings);
            }
        }
        if(StrUtil.isNotBlank(alias)){
            indexRequest.alias(new Alias(String.valueOf(alias)));
        }
        return restHighLevelClient.indices().create(indexRequest).isAcknowledged();
    }

    /**
     * description: 给已存在的index设置mapping
     * @param:
    * @param indexName
    * @param type
    * @param mappings
     * @return {@link boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public boolean putMapping(String indexName, String type, Object mappings) throws IOException{
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.type(type);
        if (mappings instanceof String) {
            putMappingRequest.source(type , String.valueOf(mappings), XContentType.JSON);
        } else if (mappings instanceof Map) {
            putMappingRequest.source((Map) mappings);
        } else if (mappings instanceof XContentBuilder) {
            putMappingRequest.source((XContentBuilder) mappings);
        }
        return restHighLevelClient.indices().putMapping(putMappingRequest).isAcknowledged();
    }


    /**
     * description: 返回条件查询出来的总数据量
     * @param:
    * @param searchRequest
     * @return {@link Long}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Long count(SearchRequest searchRequest) throws IOException{
        return restHighLevelClient.search(searchRequest).getHits().getTotalHits();
    }

    /**
     * description: 保存或更新
     * @param:
    * @param indexRequest
     * @return {@link String}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public String index(IndexRequest indexRequest) throws IOException{
        return restHighLevelClient.index(indexRequest).getId();
    }

    /**
     * description: 更新
     * @param:
    * @param indexRequest
     * @return {@link String}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public String update(UpdateRequest updateRequest) throws IOException{
        return restHighLevelClient.update(updateRequest).getId();
    }

    /**
     * description: 删除doc
     * @param:
    * @param indexName
    * @param type
    * @param id
     * @return {@link String}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public String delete(String indexName, String type, String id) throws IOException{
        return restHighLevelClient.delete(new DeleteRequest(indexName , type , id)).getId();
    }

    /**
     * description: 删除索引
     * @param:
    * @param indexName
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Boolean deleteIndex(String indexName) throws IOException{
        return restHighLevelClient.indices().delete(new DeleteIndexRequest(indexName)).isAcknowledged();
    }

    /**
     * description: 批量添加或覆盖
     * @param:
    * @param queries
     * @return {@link }
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public void bulkIndex(String indexName, String typeName, List<Docs> resultList) throws IOException{
        BulkRequest bulkRequest = new BulkRequest();
        resultList.forEach(docs -> bulkRequest.add(new IndexRequest(indexName, typeName , docs.remove("_id").toString()).source(JSON.toJSONString(docs), XContentType.JSON)));
        this.checkForBulkUpdateFailure(bulkRequest);
    }

    /**
     * description: 批量添加或覆盖
     * @param:
    * @param queries
     * @return {@link }
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public void bulkIndex(BulkRequest bulkRequest) throws IOException{
        this.checkForBulkUpdateFailure(bulkRequest);
    }

    /**
     * description: 批量更新
     * @param:
    * @param queries
     * @return {@link }
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public void bulkUpdate(String indexName, String typeName, List<Docs> resultList) throws IOException{
        BulkRequest bulkRequest = new BulkRequest();
        resultList.forEach(docs -> bulkRequest.add(new UpdateRequest(indexName, typeName , docs.remove("_id").toString()).doc(docs)));
        this.checkForBulkUpdateFailure(bulkRequest);
    }

    /**
     * description: 索引是否存在
     * @param: 
    * @param indexName
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Boolean indexExists(String indexName) throws IOException{
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexName);
        return restHighLevelClient.indices().exists(getIndexRequest);
    }

    /**
     * description: type是否存在
     * @param:
    * @param indexName
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Boolean typeExists(String indexName , String type) throws IOException{
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexName);
        getIndexRequest.types(type);
        return restHighLevelClient.indices().exists(getIndexRequest);
    }

    /**
     * description: 可以刷新多个索引，如果不传则刷新全部索引
     * @param:
    * @param indexName
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public void refresh(String... indexName) throws IOException{
        RefreshRequest refreshRequest = new RefreshRequest(indexName);
        restHighLevelClient.indices().refresh(refreshRequest);
    }

    /**
     * description: 别名是否存在
     * @param:
    * @param alias
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Boolean aliasExists(String alias) throws IOException{
        return restHighLevelClient.indices().existsAlias(new GetAliasesRequest(alias));
    }

    /**
     * description: 索引和别名之间的操作
     * @param: 
    * @param indexName
    * @param alias
    * @param type {@link IndicesAliasesRequest.AliasActions.Type}
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Boolean genericAlias(String indexName , String alias , IndicesAliasesRequest.AliasActions.Type type) throws IOException{
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(type).indices(indexName).alias(alias));
        return restHighLevelClient.indices().updateAliases(indicesAliasesRequest).isAcknowledged();
    }

    /**
     * description: 将别名从旧的索引绑定到新的索引
     * @param: 
    * @param oldIndexName
    * @param newIndexName
    * @param aliases
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Boolean aliaseChange(String oldIndexName, String newIndexName, String aliases) throws IOException{
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActionsRemove = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE).indices(oldIndexName).alias(aliases);
        IndicesAliasesRequest.AliasActions aliasActionsAdd = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).indices(newIndexName).alias(aliases);
        indicesAliasesRequest.addAliasAction(aliasActionsRemove).addAliasAction(aliasActionsAdd);
        return restHighLevelClient.indices().updateAliases(indicesAliasesRequest).isAcknowledged();
    }

    /**
     * description: 条件搜索
     * @param:
    * @param searchRequest
    * @param resultsExtractor
     * @return {@link T}
     * createdBy:wanhao
     * created:2018/11/29
     * */
    public <T> T query(SearchRequest searchRequest , ResultsExtractor<T> resultsExtractor) throws IOException{
        return resultsExtractor.extract(restHighLevelClient.search(searchRequest));
    }

    /**
     * description: 滚动查询
     * @param: 
    * @param timeValue 时间
    * @param searchRequest 条件
    * @param indexName
    * @param clazz 返回参数
    * @param mapper 返回后的结果处理
     * @return {@link Page <T>}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public <T> Page<T> startHighScroll(SearchRequest searchRequest , Class<T> clazz, SearchResultMapper mapper) throws IOException{
        return mapper.mapResults(restHighLevelClient.search(searchRequest), clazz, null);
    }

    /**
     * description: 后续的滚动处理
     * @param: 
    * @param scrollId
    * @param scrollTimeInMillis
    * @param clazz
    * @param mapper
     * @return {@link Page <T>}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public <T> Page<T> continueScroll(String scrollId, long scrollTimeInMillis, Class<T> clazz, SearchResultMapper mapper) throws IOException{
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
        searchScrollRequest.scroll(TimeValue.timeValueMillis(scrollTimeInMillis));
        return mapper.mapResults(restHighLevelClient.searchScroll(searchScrollRequest), clazz, null);
    }

    /**
     * description: 清除滚动ID
     * @param:
    * @param scrollId
     * @return {@link Boolean}
     * createdBy:wanhao
     * created:2018/11/28
     * */
    public Boolean clearScroll(String scrollId) throws IOException{
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        return restHighLevelClient.clearScroll(clearScrollRequest).isSucceeded();
    }

    private void checkForBulkUpdateFailure(BulkRequest bulkRequest) throws IOException{
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest);
        if (bulkResponse.hasFailures()) {
            Map<String, String> failedDocuments = new HashMap<>();
            for (BulkItemResponse item : bulkResponse.getItems()) {
                if (item.isFailed())
                    failedDocuments.put(item.getId(), item.getFailureMessage());
            }
            throw new ElasticsearchException(
                    "Bulk indexing has failures. Use ElasticsearchException.getFailedDocuments() for detailed messages ["
                            + failedDocuments + "]",
                    failedDocuments);
        }
    }
}
