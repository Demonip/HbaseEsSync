package org.eminem.hbase.observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Demonip on 2017/9/8.
 */
public class BuildMapping {
    private static final Log LOG = LogFactory.getLog(BuildMapping.class);
    //创建Mapping
    public static Boolean buildMapping(Client client, String indexName, String typeName) throws IOException {
        client.admin().indices().prepareCreate(indexName).execute().actionGet();
        PutMappingRequestBuilder builder = client.admin().indices().preparePutMapping(indexName);
        builder.setType(typeName);
        XContentBuilder mapping = putMapping();
        builder.setSource(mapping);
        PutMappingResponse response = builder.execute().actionGet();
        return response.isAcknowledged();
    }

    //设置Mapping
    public static XContentBuilder putMapping( ) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("properties")
                   .startObject("eprincitype")
                     .field("type","integer")
                     .field("index","not_analyzed")
                   .endObject()
                   .startObject("einduclassi")
                     .field("type","integer")
                     .field("index","not_analyzed")
                   .endObject()
                   .startObject("eregcap")
                     .field("type","float")
                     .field("index","not_analyzed")
                   .endObject()
                   .startObject("eparlegalpercardtype")
                     .field("type","string")
                     .field("index","not_analyzed")
                   .endObject()
                   .startObject("ename_c")
                     .field("type","string")
                     .field("analyzer","ik")
                     .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("elegalper")
                      .field("type","string")
                      .field("analyzer","ik")
                      .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("eaddres")
                       .field("type","string")
                       .field("analyzer","ik")
                       .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("eregistryname")
                       .field("type","string")
                       .field("analyzer","ik")
                       .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("ebusiscope")
                       .field("type","string")
                       .field("analyzer","ik")
                       .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("eparname")
                       .field("type","string")
                       .field("analyzer","ik")
                       .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("edeactiveunitname")
                       .field("type","string")
                       .field("analyzer","ik")
                       .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("elogoutunitname")
                       .field("type","string")
                       .field("analyzer","ik")
                       .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("eparaddr")
                       .field("type","string")
                       .field("analyzer","ik")
                       .field("search_analyzer","ik_smart")
                   .endObject()
                   .startObject("esubcominfo")
                     .field("type","nested")
                        .startObject("properties")
                           .startObject("legalperscardtype")
                               .field("type","string")
                               .field("index","not_analyzed")
                           .endObject()
                           .startObject("cname")
                                .field("type","string")
                                .field("analyzer","ik")
                                .field("search_analyzer","ik_max_word")
                           .endObject()
                        .endObject()
                    .endObject()
                    .startObject("epersonnel")
                        .field("type","nested")
                            .startObject("properties")
                                .startObject("cardtype")
                                   .field("type","string")
                                   .field("index","not_analyzed")
                                .endObject()
                                .startObject("titles")
                                    .field("type","string")
                                    .field("index","not_analyzed")
                                .endObject()
                                .startObject("worktype")
                                    .field("type","string")
                                    .field("index","not_analyzed")
                                .endObject()
                                .startObject("nation")
                                    .field("type","string")
                                    .field("index","not_analyzed")
                                .endObject()
                                .startObject("name")
                                    .field("type","string")
                                    .field("analyzer","ik")
                                    .field("search_analyzer","ik_max_word")
                                .endObject()
                                .startObject("address")
                                    .field("type","string")
                                    .field("analyzer","ik")
                                    .field("search_analyzer","ik_smart")
                                .endObject()
                            .endObject()
                    .endObject()
                    .startObject("einvestorinfo")
                        .field("type","nested")
                        .startObject("properties")
                            .startObject("investment")
                                .field("type","float")
                                .field("index","not_analyzed")
                            .endObject()
                            .startObject("ratio")
                                .field("type","float")
                                .field("index","not_analyzed")
                            .endObject()
                            .startObject("investortype")
                                .field("type","string")
                                .field("index","not_analyzed")
                            .endObject()
                            .startObject("investtype")
                               .field("type","string")
                               .field("index","not_analyzed")
                            .endObject()
                            .startObject("name")
                                .field("type","string")
                                .field("analyzer","ik")
                                .field("search_analyzer","ik_max_word")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("ebusiplaceinfo")
                        .field("type","nested")
                        .startObject("properties")
                            .startObject("addr")
                                .field("type","string")
                                .field("analyzer","ik")
                                .field("search_analyzer","ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        return mapping;
    }
}
