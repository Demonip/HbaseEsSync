package org.eminem.hbase.observer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ESClient {
    public  static String clusterName;
    public  static String nodeHost;
    public  static int nodePort;
    public  static String indexName;
    public  static String typeName;
    public  static TransportClient client;//要配置的所有es端口信息

    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : ESClient.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }

    public static void initEsClient() throws UnknownHostException {
        final Log LOG = LogFactory.getLog(ESClient.class);
        /**连接es，将集群名重新命名，启动嗅探*/
        Settings settings = Settings.builder()
                .put("cluster.name", ESClient.clusterName)
                .put("client.transport.sniff", true).build();
        client =  TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeHost), 9300));//连接主节点
        List<DiscoveryNode> nodesList = client.connectedNodes();
        /**打印节点信息*/
        for ( DiscoveryNode node : nodesList) {
            LOG.info("ES nodes ： " + node.getHostAddress());
        }
    }
/**关闭client*/
    public static void closeEsClient() {
        client.close();
    }

}

