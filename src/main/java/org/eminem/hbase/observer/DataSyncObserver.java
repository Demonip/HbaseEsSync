package org.eminem.hbase.observer;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;
import static org.eminem.hbase.observer.ESClient.client;
import static org.eminem.hbase.observer.ESClient.indexName;
import static org.eminem.hbase.observer.ESClient.typeName;

/*
*数据同步接口
*/
public class DataSyncObserver extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(DataSyncObserver.class);

    private void readConfiguration(CoprocessorEnvironment env) {
        /**取得绑定observer命令中的端口信息*/
        Configuration conf = env.getConfiguration();
        ESClient.clusterName = conf.get("es_cluster");
        ESClient.nodeHost = conf.get("es_host");
        ESClient.nodePort = conf.getInt("es_port", -1);
        ESClient.indexName = conf.get("es_index");
        ESClient.typeName = conf.get("es_type");
        LOG.info("observer -- started with config: " + ESClient.getInfo());

    }

/*
*启动es客户端
*/
    public void start(CoprocessorEnvironment env) throws IOException {
        /**获取端口信息*/
        readConfiguration(env);
        /**初始化es客户端*/
        ESClient.initEsClient();
        /**日志打印信息*/
        LOG.info("------observer init EsClient successfully------"+ ESClient.getInfo());
        //判断ES中是否存在索引，没有则创建
        if (!indexExists(indexName, client)){
            try {
                if(BuildMapping.buildMapping(client,indexName,typeName)){
                    LOG.info("Create ES Index successfully ! IndexName : " + indexName +" TypeName :" + typeName);
                }
                else {
                    LOG.error("Create ES Index failed ! IndexName : " + indexName +" TypeName :" + typeName);
                }
            }catch (IOException e) {
                LOG.error("Create ES failed! IndexName : " + indexName);
                e.printStackTrace();
            }
        }
    }

/*
* 关闭es客户端
*/
    public void stop(CoprocessorEnvironment e) throws IOException {
        /**结束es客户端*/
        ESClient.closeEsClient();
        /**关闭es的bulkoperator*/
        ElasticSearchBulkOperator.shutdownScheduEx();
    }

/*
* 数据put进入hbase后触发postPut，用于往es同步数据
*/
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) {

        //id对应hbase中的row
        String indexId = new String(put.getRow());
        try {
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            LOG.info("familyMap -> "+familyMap.toString());

            //将所用key改为小写后的JsonObject
            JSONObject newJson = new JSONObject();
            newJson.put("id",indexId);

            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {

                for (Cell cell : entry.getValue()) {
                    //获取字段
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();

                    //获取对应字段的值
                    String value = Bytes.toString(CellUtil.cloneValue(cell));

                    //这四个字段都是jsonarray的数据格式，需要为jsonArray格式的数据转化成小写再插入es，
                    //如果出现值为空的字段，则只需要一般处理即可
                    if(key.equals("ebusiplaceinfo")||key.equals("einvestorinfo")||
                            key.equals("epersonnel")||key.equals("esubcominfo")) {

                        JSONArray jsonArray = JSON.parseArray(value);
                        JSONArray changedJsonArray = new JSONArray();

                        try {
                            //将value转为key为小写格式的Jsonarray
                            for (int i = 0 ; i < jsonArray.size() ; i++) {
                                JSONObject changeJson = jsonArray.getJSONObject(i);
                                JSONObject lowerJson = new JSONObject();
                                Set<String> set = changeJson.keySet();
                                for(String keyOfJson : set){
                                    String subKey = keyOfJson.toLowerCase();
                                    if (ConstantUtils.getIntegKey().contains(subKey)) {
                                        lowerJson.put(subKey,Integer.valueOf(changeJson.getString(keyOfJson).trim()));
                                    }
                                    else if (ConstantUtils.getFloKey().contains(subKey.toString())) {
                                        lowerJson.put(subKey,Float.valueOf(changeJson.getString(keyOfJson).trim()));
                                    }
                                    else {
                                        lowerJson.put(subKey,changeJson.get(keyOfJson.toString()));
                                    }
                                }
                                changedJsonArray.add(lowerJson);
                            }
                            newJson.put(key,changedJsonArray);
                        } catch (JSONException e1) {
                            e1.printStackTrace();
                            LOG.error("change value for JsonArray error! ");
                        }
                     }
                     else {
                        if (ConstantUtils.getIntegKey().contains(key.toString())) {
                            newJson.put(key,Integer.valueOf(value.trim()));
                        }
                        else if (ConstantUtils.getFloKey().contains(key.toString())) {
                            newJson.put(key,Float.valueOf(value.trim()));
                        }
                        else {
                            newJson.put(key,value);
                         }
                    }
                }
                    LOG.info("Sync data Creditcoed is : " + put.getRow().toString() + ",put data is : " + newJson.toString());
             }
            //将如上产生的完整json串插入es中
            BulkRequestBuilder bulkRequestmy = client.prepareBulk();
            bulkRequestmy.add(client.prepareIndex(indexName,typeName).setId(indexId).setSource(newJson));
            bulkRequestmy.execute().actionGet();
         } catch (Exception ex) {
            LOG.error("observer put a doc, index [ " + ESClient.indexName + " ] " + "indexId [" + indexId + "] error : " + ex.getMessage());
            ex.printStackTrace();
         }
    }
/*
*删除操作，令es和hbase中的数据同步删除
*/
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(delete.getRow());
        try {
            ElasticSearchBulkOperator.addDeleteBuilderToBulk(client.prepareDelete(ESClient.indexName, ESClient.typeName, indexId));
        } catch (Exception ex) {
            LOG.error(ex);
            LOG.error("observer delete  a doc, index [ " + ESClient.indexName + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());
        }
    }

/*
*判断es中的索引是否存在
*/
    public  static boolean indexExists(String index, Client client) {
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }
}

