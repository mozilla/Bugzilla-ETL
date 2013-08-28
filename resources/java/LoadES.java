import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

private BulkRequestBuilder bulk = null;
private Client client;
private String esNodes = "127.0.0.1:9300";
private String esCluster = "elasticsearch";
private String esIndex = "bugs";
private int batch = 0;
private static int BULK_LOAD_MAX = 1000;
private static int BULK_TIMEOUT = 30000;

public String paramOrDefault(String parameterName, String defaultValue)
{
    String paramValue = getParameter(parameterName);
    if (paramValue != null && paramValue.length() > 0 && !paramValue.equals("${" + parameterName + "}"))
    {
        return paramValue;
    }
    return defaultValue;
}
public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
{
    batch++;
    Object[] r = getRow();

    // FIXME: use bulk loader.

    if (r == null) {

        if (bulk != null) {
            //BulkResponse response = (BulkResponse)bulk.execute().actionGet(BULK_TIMEOUT);
            BulkResponse response = (BulkResponse)bulk.execute().actionGet();
            if (response.hasFailures()) {
               System.out.println("ES Response had failures:");
               Iterator iterator = response.iterator();
                while (iterator.hasNext()) {
                    BulkItemResponse responseItem = (BulkItemResponse)iterator.next();
                    if (responseItem.failed()) {
                        System.out.println("Indexing failed for '" + responseItem.getId()
                                + "': " + responseItem.getFailureMessage());
                    }
                }
            }
            System.out.println("Loaded " + batch + " docs in " + response.getTookInMillis() + " ms");
        }

        setOutputDone();
        return false;
    }

    if (first)
    {
        first = false;

        // get esNodes and esCluster from parameters (if available)
        esNodes = paramOrDefault("ES_NODES", esNodes);
        esCluster = paramOrDefault("ES_CLUSTER", esCluster);
        esIndex = paramOrDefault("ES_INDEX", esIndex);
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        settings.put("cluster.name", esCluster);
        TransportClient transportClient = new TransportClient(settings.build());

        String[] nodes = esNodes.split(",");
        for (int i = 0; i < nodes.length; i++)
        {
            String esNode = nodes[i].trim();
            int colon = esNode.indexOf(':');
            String host = esNode.substring(0, colon);
            int port = Integer.parseInt(esNode.substring(colon+1));
            TransportAddress a = new InetSocketTransportAddress(host, port);
            transportClient.addTransportAddress(a);
            System.out.println("Adding ElasticSearch Node " + host + ":" + port);
        }

        client = transportClient;
    }

    r = createOutputRow(r, data.outputRowMeta.size());

    if (batch >= BULK_LOAD_MAX) {
        //BulkResponse response = (BulkResponse)bulk.execute().actionGet(BULK_TIMEOUT);
        BulkResponse response = (BulkResponse)bulk.execute().actionGet();
        if (response.hasFailures()) {
           System.out.println("ES Response had failures:");
           Iterator iterator = response.iterator();
            while (iterator.hasNext()) {
                BulkItemResponse responseItem = (BulkItemResponse)iterator.next();
                if (responseItem.failed()) {
                    System.out.println("Indexing failed for '" + responseItem.getId()
                            + "': " + responseItem.getFailureMessage());
                }
            }
        }
        System.out.println("Loaded " + batch + " docs in " + response.getTookInMillis() + " ms");

        batch = 0;
        bulk = null;
    }

    if (bulk == null) {
        bulk = client.prepareBulk();
    }
    IndexRequestBuilder builder = client.prepareIndex(esIndex, "bug_version", get(Fields.In, "bug_version_id").getString(r));
    builder.setSource(get(Fields.In, "bug_version_json").getString(r));
//    if (!"undefined".equals(get(Fields.In, "bug_version_parent_id").getString(r)))
//        builder.setParent(get(Fields.In, "bug_version_parent_id").getString(r));

    IndexRequest ir = (IndexRequest)builder.request();

    bulk.add(ir);

/*
    IndexRequestBuilder builder = client.prepareIndex(esIndex, "bug_version", get(Fields.In, "bug_version_id").getString(r));
    builder.setSource(get(Fields.In, "bug_version_json").getString(r));
//    if (!"undefined".equals(get(Fields.In, "bug_version_parent_id").getString(r)))
//        builder.setParent(get(Fields.In, "bug_version_parent_id").getString(r));

    try {
    builder.execute().actionGet();
        System.out.println("Successfully indexed bug version "
                   + get(Fields.In, "bug_version_id").getString(r));
    } catch (Exception e) {
        System.out.println("Exception processing bug version "
                   + get(Fields.In, "bug_id").getString(r) + "_"
                   + get(Fields.In, "record_num").getString(r) + " - "
                   + e);
        throw e;
    }
*/
    return true;
}
