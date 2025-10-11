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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import dev.rkoch.aws.collector.utils.State;
import dev.rkoch.aws.s3.parquet.S3Parquet;
import software.amazon.awssdk.regions.Region;

public class NewsCollector {

  // https://open-platform.theguardian.com/documentation/search
  // https://content.guardianapis.com/search?api-key=<API_KEY>&show-fields=bodyText&lang=en&from-date=2005-08-15&to-date=2005-08-15&page=2
  private static final String API_URL =
      "https://content.guardianapis.com/search?api-key=%s&show-fields=bodyText&lang=en&page-size=50&from-date=%s&to-date=%s&page=%s";

  private static final String BUCKET_NAME = "dev-rkoch-spre";

  private static final String PARQUET_KEY = "raw/news/localDate=%s/data.parquet";

  private static final String PILLAR_NEWS = "pillar/news";

  private static final String THEGUARDIAN_API_KEY = "THEGUARDIAN_API_KEY";

  private static final long MAX_TIME_MILLIS = 14 * 60 * 1000; // 14 min

  private final LambdaLogger logger;

  private final Region region;

  private final long startTimeMillis;

  private String apiKey;

  private HttpClient httpClient;

  private long lastRequest = 0;

  private S3Parquet s3Parquet;

  private State state;

  public NewsCollector(LambdaLogger logger, Region region) {
    this.logger = logger;
    this.region = region;
    startTimeMillis = System.currentTimeMillis();
  }

  public void collect() {
    try (State state = getState()) {
      LocalDate date = getStartDate();
      LocalDate now = LocalDate.now();
      for (; continueExecution() && date.isBefore(now); date = date.plusDays(1)) {
        try {
          List<NewsRecord> records = getData(date);
          insert(date, records);
          state.setLastAddedNewsDate(date);
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
  }

  private boolean continueExecution() {
    return (System.currentTimeMillis() - startTimeMillis) <= MAX_TIME_MILLIS;
  }

  private String getApiKey() {
    if (apiKey == null) {
      apiKey = System.getenv(THEGUARDIAN_API_KEY);
    }
    return apiKey;
  }

  private List<NewsRecord> getData(final LocalDate date) throws LimitExceededException {
    List<NewsRecord> items = new ArrayList<>();
    try {
      JSONObject response = getJson(date, 1);
      int pages = response.getInt("pages");
      items.addAll(getItems(date, response));
      for (int page = 2; page <= pages; page++) {
        items.addAll(getItems(date, getJson(date, page)));
      }
      return items;
    } catch (JSONException e) {
      throw new LimitExceededException();
    }
  }

  private HttpClient getHttpClient() {
    if (httpClient == null) {
      httpClient = HttpClient.newHttpClient();
    }
    return httpClient;
  }

  private List<NewsRecord> getItems(final LocalDate date, final JSONObject response) {
    List<NewsRecord> items = new ArrayList<>();
    JSONArray results = response.getJSONArray("results");
    for (int i = 0; i < results.length(); i++) {
      JSONObject result = results.getJSONObject(i);
      try {
        String pillarId = result.getString("pillarId");
        if (PILLAR_NEWS.equalsIgnoreCase(pillarId)) {
          String body = result.getJSONObject("fields").getString("bodyText");
          if (!body.isBlank()) {
            String id = result.getString("id");
            String title = result.getString("webTitle");
            items.add(NewsRecord.of(date, id, title, body));
          }
        }
      } catch (JSONException e) {
        continue;
      }
    }
    return items;
  }

  private JSONObject getJson(final LocalDate date, final int page) {
    try {
      HttpRequest httpRequest = HttpRequest.newBuilder(getUri(date, date, page)).build();
      waitBeforeApiCall();
      HttpResponse<String> httpResponse = getHttpClient().send(httpRequest, BodyHandlers.ofString());
      String body = httpResponse.body();
      return new JSONObject(body).getJSONObject("response");
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private S3Parquet getS3Parquet() {
    if (s3Parquet == null) {
      s3Parquet = new S3Parquet(region);
    }
    return s3Parquet;
  }

  private LocalDate getStartDate() {
    LocalDate lastAddedNewsDate = getState().getLastAddedNewsDate();
    if (lastAddedNewsDate == null) {
      return getState().getAvStartDate();
    } else {
      return lastAddedNewsDate.plusDays(1);
    }
  }

  private State getState() {
    if (state == null) {
      state = new State(region, BUCKET_NAME);
    }
    return state;
  }

  private URI getUri(final LocalDate from, final LocalDate to, final int page) {
    return URI.create(API_URL.formatted(getApiKey(), from.toString(), to.toString(), page));
  }

  private void insert(final LocalDate date, final List<NewsRecord> items) throws Exception {
    getS3Parquet().write(BUCKET_NAME, PARQUET_KEY.formatted(date), items, new NewsRecord().getDehydrator());
  }

  private void waitBeforeApiCall() throws InterruptedException {
    while (System.currentTimeMillis() - lastRequest < 1000) {
      Thread.sleep(50);
    }
    lastRequest = System.currentTimeMillis();
  }

}
