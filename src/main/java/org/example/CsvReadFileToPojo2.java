package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.SerializableFunction;
import org.example.pojo.Employee;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class CsvReadFileToPojo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /////////////////////////////////////////// Test Code 1/////////////////////
        SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = mapper ->
                mapper.schemaFor(Employee.class).withoutQuoteChar().withColumnSeparator(',');

        CsvReaderFormat<Employee> csvReaderFormat =
                CsvReaderFormat.forSchema(CsvMapper::new, schemaGenerator, TypeInformation.of(Employee.class));
        /////////////////////////////////////////////////////////////////////

        String path = "src\\main\\resources";
        final FileSource<Employee> source =
                FileSource.forRecordStreamFormat(csvReaderFormat,new Path(path))
                        .build();

        final DataStream<Employee> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        stream.print();
        env.execute();
    }
}
