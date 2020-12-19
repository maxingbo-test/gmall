package com.gmall.gmallpublisher.service.imple;

import com.gmall.gmallpublisher.service.EsService;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EsServiceImpl implements EsService {

    @Resource
    JestClient jestClient;

    @Override
    public Long getDauTotal(String dt) {
        String indexName = "gmall_dau_info_"+dt+"-query";
//        String searchAction="{\n" +
//                "  \"query\": {\n" +
//                "    \"match_all\": {}\n" +
//                "  }\n" +
//                "}";
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(sourceBuilder.toString())
                                  .addIndex(indexName)
                                  .addType("_doc")
                                  .build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            jestClient.close();
            return searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常......");
        }
    }

    @Override
    public Map getDauHour(String dt) {
        String indexName = "gmall_dau_info_"+dt+"-query";
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggbuilder =
                AggregationBuilders.terms("group_by_hr").field("hr").size(24);
        sourceBuilder.aggregation(aggbuilder);
//        String source =
//                "{\n" +
//                "  \"aggs\": {\n" +
//                "    \"group_by_hr\": {\n" +
//                "      \"terms\": {\n" +
//                "        \"field\": \"hr\"\n" +
//                "        ,\"size\": 24\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}";
        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(indexName)
                                            .addType("_doc")
                                            .build();
        try {
            SearchResult result = jestClient.execute(search);
            if(result != null){
                Map<String,Long> _map = new HashMap<>();
                List<TermsAggregation.Entry> groupByHr = result.getAggregations()
                                            .getTermsAggregation("group_by_hr")
                                            .getBuckets();
                for (TermsAggregation.Entry bucket : groupByHr) {
                    _map.put(bucket.getKey(),bucket.getCount());
                }
                return _map;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
