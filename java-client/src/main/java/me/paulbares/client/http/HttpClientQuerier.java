package me.paulbares.client.http;

import feign.Feign;
import feign.Headers;
import feign.RequestLine;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.dto.MetadataResultDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.QueryResultDto;
import okhttp3.OkHttpClient;

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

  public MetadataResultDto metadata() {
    QueryApi target = builder.target(QueryApi.class, this.url);
    return target.metadata();
  }

  interface QueryApi {
    @RequestLine("POST /query")
    @Headers("Content-Type: application/json")
    QueryResultDto run(QueryDto query);

    @RequestLine("GET /metadata")
    MetadataResultDto metadata();
  }
}
