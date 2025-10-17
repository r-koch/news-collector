package dev.rkoch.aws.news.collector;

import java.io.IOException;
import java.net.URI;
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

public class NewsCollector {

  // https://open-platform.theguardian.com/documentation/search
  // https://content.guardianapis.com/search?api-key=<API_KEY>&show-fields=bodyText&lang=en&from-date=2005-08-15&to-date=2005-08-15&page=2
  private static final String API_URL =
      "https://content.guardianapis.com/search?api-key=%s&show-fields=bodyText&lang=en&page-size=50&from-date=%s&to-date=%s&page=%s";

  private static final String BUCKET_NAME = "dev-rkoch-spre";

  private static final String PARQUET_KEY = "raw/news/localDate=%s/data.parquet";

  private static final String PILLAR_NEWS = "pillar/news";

  private static final long MAX_TIME_MILLIS = 14 * 60 * 1000; // 14 min

  private final LambdaLogger logger;

  private final Handler handler;

  private final long startTimeMillis;

  private long lastRequest = 0;

  public NewsCollector(LambdaLogger logger, Handler handler) {
    this.logger = logger;
    this.handler = handler;
    startTimeMillis = System.currentTimeMillis();
  }

  public void collect() {
    try (State state = new State(handler.getS3Client(), BUCKET_NAME)) {
      LocalDate date = getStartDate(state);
      LocalDate now = LocalDate.now();
      for (; continueExecution() && date.isBefore(now); date = date.plusDays(1)) {
        try {
          List<NewsRecord> records = getData(date);
          if (records.isEmpty()) {
            logger.log("%s no data".formatted(date), LogLevel.INFO);
          } else {
            insert(date, records);
            logger.log("%s inserted".formatted(date), LogLevel.INFO);
          }
          state.setLastAddedNewsDate(date);
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
      HttpResponse<String> httpResponse = handler.getHttpClient().send(httpRequest, BodyHandlers.ofString());
      String body = httpResponse.body();
      return new JSONObject(body).getJSONObject("response");
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private LocalDate getStartDate(final State state) {
    LocalDate lastAddedNewsDate = state.getLastAddedNewsDate();
    if (lastAddedNewsDate == null) {
      return state.getAvStartDate();
    } else {
      return lastAddedNewsDate.plusDays(1);
    }
  }

  private URI getUri(final LocalDate from, final LocalDate to, final int page) {
    return URI.create(API_URL.formatted(handler.getApiKey(), from.toString(), to.toString(), page));
  }

  private void insert(final LocalDate date, final List<NewsRecord> items) throws Exception {
    handler.getS3Parquet().write(BUCKET_NAME, PARQUET_KEY.formatted(date), items);
  }

  private void waitBeforeApiCall() throws InterruptedException {
    while (System.currentTimeMillis() - lastRequest < 1000) {
      Thread.sleep(50);
    }
    lastRequest = System.currentTimeMillis();
  }

}
