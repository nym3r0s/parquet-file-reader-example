package com.gokul.parquet;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ParquetApplication {

  public static void main(String[] args) throws URISyntaxException, IOException {
    SpringApplication.run(ParquetApplication.class, args);
    readParquetAndPrint();
  }


  private static void readParquetAndPrint() throws URISyntaxException, IOException {
    File file =new File(ParquetApplication.class.getClassLoader().getResource("part-00000-42da1110-5dde-4a06-92d2-8cc3805e9acc-c000.snappy.parquet").toURI());
    Path path = new Path(file.toURI());

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(path);
    GenericRecord record;
    record = reader.read();
    System.out.println(record.getSchema().toString());
    do {
      System.out.println("store_address_id: "+ record.get("store_address_id"));
      System.out.println("p_creation_date: "+ record.get("p_creation_date"));
      System.out.println("number_views: "+ record.get("number_views"));
      System.out.println("number_orders: "+ record.get("number_orders"));
    } while ((record = reader.read()) != null);
  }
}
