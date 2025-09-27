package dev.rkoch.aws.news.collector;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import javax.naming.LimitExceededException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rkoch.aws.s3.parquet.S3Parquet;
import dev.rkoch.aws.utils.log.SystemOutLambdaLogger;

public class NewsCollector {

  // https://open-platform.theguardian.com/documentation/search
  // https://content.guardianapis.com/search?api-key=<API_KEY>&show-fields=bodyText&lang=en&from-date=2005-08-15&to-date=2005-08-15&page=2
  private static final String API_URL =
      "https://content.guardianapis.com/search?api-key=%s&show-fields=bodyText&lang=en&page-size=50&from-date=%s&to-date=%s&page=%s";

  private static final String BUCKET_NAME = "dev-rkoch-spre";

  private static final String PARQUET_KEY = "raw/news/localDate=%s/data.parquet";

  private static final String PILLAR_NEWS = "pillar/news";

  private static final String THEGUARDIAN_API_KEY = "THEGUARDIAN_API_KEY";

  public static void main(String[] args) {
    new NewsCollector(new SystemOutLambdaLogger()).collect();
  }

  private final LambdaLogger logger;

  private String apiKey;

  private HttpClient httpClient;

  private ObjectMapper objectMapper;

  private S3Parquet s3Parquet;

  public NewsCollector(LambdaLogger logger) {
    this.logger = logger;
  }

  public void collect() {
    LocalDate date = getStartDate();
    LocalDate now = LocalDate.now();
    for (; date.isBefore(now); date = date.plusDays(1)) {
      try {
        List<NewsRecord> records = getData(date);
        insert(date, records);
        Variable.DATE_LAST_ADDED_NEWS.set(date.toString());
        logger.log("%s inserted".formatted(date), LogLevel.INFO);
      } catch (LimitExceededException e) {
        logger.log("theguardian limit exceeded".formatted(date), LogLevel.INFO);
        return;
      } catch (Exception e) {
        logger.log(e.getMessage(), LogLevel.ERROR);
        return;
      }
    }
  }

  private LocalDate getStartDate() {
    String lastAdded = Variable.DATE_LAST_ADDED_NEWS.get();
    if (lastAdded.isBlank()) {
      return LocalDate.parse(Variable.DATE_START_AV.get());
    } else {
      return LocalDate.parse(lastAdded).plusDays(1);
    }
  }

  private String getApiKey() {
    if (apiKey == null) {
      apiKey = System.getenv(THEGUARDIAN_API_KEY);
    }
    return apiKey;
  }

  private List<NewsRecord> getData(final LocalDate date) throws LimitExceededException {
    List<NewsRecord> items = new ArrayList<>();
    JsonNode response = getJson(date, 1);
    JsonNode pages = response.findValue("pages");
    if (pages == null) {
      throw new LimitExceededException();
    } else {
      int pageCount = pages.asInt();
      items.addAll(getItems(date, response));
      for (int i = 2; i <= pageCount; i++) {
        items.addAll(getItems(date, getJson(date, i)));
      }
      return items;
    }
  }

  private HttpClient getHttpClient() {
    if (httpClient == null) {
      httpClient = HttpClient.newHttpClient();
    }
    return httpClient;
  }

  private List<NewsRecord> getItems(final LocalDate date, final JsonNode response) throws LimitExceededException {
    List<NewsRecord> items = new ArrayList<>();
    JsonNode results = response.findValue("results");
    if (results == null) {
      throw new LimitExceededException();
    } else {
      for (JsonNode result : results) {
        JsonNode pillarIdNode = result.findValue("pillarId");
        if (pillarIdNode != null) {
          String pillarId = pillarIdNode.asText();
          if (PILLAR_NEWS.equalsIgnoreCase(pillarId)) {
            String body = result.findValue("bodyText").asText();
            if (!body.isBlank()) {
              String id = result.findValue("id").asText();
              String headline = result.findValue("webTitle").asText();
              items.add(NewsRecord.of(date, id, headline, body));
            }
          }
        }
      }
    }

    return items;
  }

  private JsonNode getJson(final LocalDate date, final int page) {
    HttpRequest httpRequest = HttpRequest.newBuilder(getUri(date, date, page)).build();
    try {
      HttpResponse<String> httpResponse = getHttpClient().send(httpRequest, BodyHandlers.ofString());
      String body = httpResponse.body();
      return getObjectMapper().readTree(body);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      objectMapper = new ObjectMapper();
    }
    return objectMapper;
  }

  private S3Parquet getS3Parquet() {
    if (s3Parquet == null) {
      s3Parquet = new S3Parquet();
    }
    return s3Parquet;
  }

  private URI getUri(final LocalDate from, final LocalDate to, final int page) {
    return URI.create(API_URL.formatted(getApiKey(), from.toString(), to.toString(), page));
  }

  private void insert(final LocalDate date, final List<NewsRecord> items) throws Exception {
    getS3Parquet().write(BUCKET_NAME, PARQUET_KEY.formatted(date), items);
  }

}
