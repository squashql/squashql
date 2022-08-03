package me.paulbares.client;

import feign.Feign;
import feign.Headers;
import feign.RequestLine;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.spring.web.rest.QueryController;
import okhttp3.OkHttpClient;

import java.util.Map;

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

  public Object run(QueryDto query) {
    QueryApi target = builder.target(QueryApi.class, this.url);
    return target.run(query);
  }

  public Map<Object, Object> metadata() {
    QueryApi target = builder.target(QueryApi.class, this.url);
    return target.metadata();
  }

  interface QueryApi {
    @RequestLine("POST " + QueryController.MAPPING_QUERY)
    @Headers("Content-Type: application/json")
    Object run(QueryDto query);

    @RequestLine("GET " + QueryController.MAPPING_METADATA)
    Map<Object, Object> metadata();
  }
}
