package com.gmall.gmallpublisher.service.imple;

import com.gmall.gmallpublisher.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.io.IOException;
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
    public Map getDauHour(String date) {
        return null;
    }

}
