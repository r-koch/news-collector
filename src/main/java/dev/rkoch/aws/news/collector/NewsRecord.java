package dev.rkoch.aws.news.collector;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import blue.strategic.parquet.Dehydrator;
import dev.rkoch.aws.s3.parquet.ParquetRecord;

public class NewsRecord implements ParquetRecord {

  public static NewsRecord of(LocalDate localDate, String id, String headline, String body) {
    return new NewsRecord(localDate, id, headline, body);
  }

  private final LocalDate localDate;
  private final String id;
  private final String headline;
  private final String body;

  public NewsRecord(LocalDate localDate, String id, String headline, String body) {
    this.localDate = localDate;
    this.id = id;
    this.headline = headline;
    this.body = body;
  }

  public String getBody() {
    return body;
  }

  public String getHeadline() {
    return headline;
  }

  public String getId() {
    return id;
  }

  public LocalDate getLocalDate() {
    return localDate;
  }

  @Override
  public MessageType getSchema() {
    return new MessageType("news-record", //
        Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("localDate"), //
        Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("id"), //
        Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("headline"), //
        Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("body") //
    );
  }

  @Override
  public Dehydrator<ParquetRecord> getDehydrator() {
    return (record, valueWriter) -> {
      valueWriter.write("localDate", Long.valueOf(ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), localDate)).intValue());
      valueWriter.write("id", id);
      valueWriter.write("headline", headline);
      valueWriter.write("body", body);
    };
  }

}
