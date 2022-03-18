package me.paulbares.query;

import me.paulbares.jackson.JacksonUtil;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ExpressionResolver {

    public static Map<String, ExpressionMeasure> get(String uri) {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            ExpressionMeasure[] measures = JacksonUtil.deserialize(response.body(), ExpressionMeasure[].class);
            Map<String, ExpressionMeasure> r = new HashMap<>();
            Arrays.stream(measures).forEach(m -> r.put(m.alias, m));
            return r;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
