package io.squashql.client.http;

import feign.*;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.Measure;
import io.squashql.query.dto.MetadataResultDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.QueryMergeDto;
import io.squashql.query.dto.QueryResultDto;
import okhttp3.OkHttpClient;

import java.util.List;

public class HttpClientQuerier {

  private static final OkHttpClient client = new OkHttpClient();

  private static final Feign.Builder builder = Feign.builder()
          .client(new feign.okhttp.OkHttpClient(client))
          .encoder(new JacksonEncoder(JacksonUtil.mapper))
          .decoder(new JacksonDecoder(JacksonUtil.mapper));

  public String url;

  public HttpClientQuerier(String url) {
    this.url = url;
  }

  public QueryResultDto run(QueryDto query) {
    QueryApi target = builder.target(QueryApi.class, this.url);
    return target.run(query);
  }

  public QueryResultDto queryMerge(QueryMergeDto query) {
    QueryApi target = builder.target(QueryApi.class, this.url);
    return target.queryMerge(query);
  }

  public MetadataResultDto metadata() {
    QueryApi target = builder.target(QueryApi.class, this.url);
    return target.metadata();
  }

  public List<Measure> expression(List<Measure> measures) {
    QueryApi target = builder.target(QueryApi.class, this.url);
    return target.expression(measures);
  }

  interface QueryApi {
    @RequestLine("POST /query")
    @Headers("Content-Type: application/json")
    QueryResultDto run(QueryDto query);

    @RequestLine("POST /query-merge")
    @Headers("Content-Type: application/json")
    QueryResultDto queryMerge(QueryMergeDto query);

    @RequestLine("GET /metadata")
    MetadataResultDto metadata();

    @RequestLine("POST /expression")
    @Headers("Content-Type: application/json")
    List<Measure> expression(List<Measure> measures);
  }
}
