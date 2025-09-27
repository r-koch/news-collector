package dev.rkoch.aws.news.collector;

import dev.rkoch.aws.utils.ddb.DefaultVariable;

public enum Variable implements DefaultVariable {

  DATE_START_AV, //
  DATE_START_NQ, //
  DATE_LAST_ADDED_NEWS, //
  ;

  @Override
  public String getTable() {
    return "STATE";
  }

}
