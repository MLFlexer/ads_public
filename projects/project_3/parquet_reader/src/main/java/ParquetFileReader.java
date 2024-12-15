import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.avro.generic.GenericRecord;
import java.io.IOException;
import java.util.List;

public class ParquetFileReader {
    public static void readSelectedColumns(String parquetFilePath) throws IOException {
        Path path = new Path(parquetFilePath);

        Schema schema = SchemaBuilder.record("projection")
                .fields()
                .optionalString("text")
                //.optionalLong("label")
                .endRecord();

        schema = null;

        ParquetReader<GenericRecord> reader;
        if (schema != null) {
          Configuration configuration = new Configuration();
          configuration.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, schema.toString());
          reader = AvroParquetReader
            .<GenericRecord>builder(path)
            .withConf(configuration)
            .build();
        } else {
          reader = AvroParquetReader
            .<GenericRecord>builder(path)
            .build();
        }
        GenericRecord record;
        while ((record = reader.read()) != null) {
          System.out.println(record);
        }
    }

    public static void main(String[] args) {
        try {
            readSelectedColumns(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
