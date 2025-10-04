package dev.rkoch.aws.news.collector;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.regions.Region;

public class Handler implements RequestHandler<Void, Void> {

  @Override
  public Void handleRequest(Void input, Context context) {
    new NewsCollector(context.getLogger(), Region.of(System.getenv("AWS_REGION"))).collect();
    return null;
  }

}
