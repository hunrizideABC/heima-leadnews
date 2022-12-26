package com.heima.es;

import com.alibaba.fastjson.JSON;
import com.heima.es.mapper.ApArticleMapper;
import com.heima.es.pojo.SearchArticleVo;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@SpringBootTest
@RunWith(SpringRunner.class)
public class ApArticleTest {

    @Autowired
    private ApArticleMapper apArticleMapper;

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * 注意：数据量的导入，如果数据量过大，需要分页导入
     * @throws Exception
     */

    @Autowired
    private EsIndexOperation esIndexOperation;
    @Test
    public void init() throws Exception {

        //1.查询所有符合条件的文章数据
        List<SearchArticleVo> searchArticleVos = apArticleMapper.loadArticleList();


        //2.创建或删除es索引库
//        esIndexOperation.deleteIndex("app_info_article");
//        esIndexOperation.createIndex("app_info_article", new HashMap<>());


        //3.导入到es索引库
//        int index=0;
//        for(SearchArticleVo searchArticleVo:searchArticleVos) {
//            if(index>10) {
//                System.out.println(searchArticleVo.getTitle());
//                IndexRequest indexRequest = new IndexRequest("app_info_article");
//                indexRequest.id(searchArticleVo.getId().toString());
//                indexRequest.source(JSON.toJSONString(searchArticleVo), XContentType.JSON);
//                restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
//            }
//            index+=1;
//        }

        //4.批量导入到es索引库

//        BulkRequest bulkRequest = new BulkRequest("app_info_article");
//
//        for (SearchArticleVo searchArticleVo : searchArticleVos) {
//
//            IndexRequest indexRequest = new IndexRequest().id(searchArticleVo.getId().toString())
//                    .source(JSON.toJSONString(searchArticleVo), XContentType.JSON);
//
//            //批量添加数据
//            bulkRequest.add(indexRequest);
//
//        }

//        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        //查询id
        GetRequest request = new GetRequest("app_info_article");

        request.id(String.valueOf(searchArticleVos.get(47).getId()));
        // 3、发送请求到ES
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("查询结果：" + response.getSourceAsString());


    }

}