package org.example;


import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.SerializableFunction;
import org.example.pojo.Employee;

import java.util.Iterator;

public class CsvReadFileToPojo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /////////////////////////////////////////// Test Code 1/////////////////////
//        SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = mapper ->
//                mapper.schemaFor(Employee.class).withoutQuoteChar().withColumnSeparator(',');

        CsvReaderFormat<Employee> csvFormat =
                CsvReaderFormat.forSchema(
                        CsvSchema.builder()
                                .addColumn(
                                        new CsvSchema.Column(0, "name", CsvSchema.ColumnType.STRING))
                                .addColumn(
                                        new CsvSchema.Column(1, "email", CsvSchema.ColumnType.STRING))

                                .build(),
                        TypeInformation.of(Employee.class));
        /////////////////////////////////////////////////////////////////////

        String path = "src\\main\\resources";
        final FileSource<Employee> source =
                FileSource.forRecordStreamFormat(csvFormat,new Path(path))
                        .build();

        final DataStream<Employee> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        CloseableIterator<String> data = stream.map(Employee::getEmail).executeAndCollect();
        //Employee employee = stream.filter(employee1 -> employee1.getEmail().isEmpty());

        //System.out.println(data.next());
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonArray = objectMapper.writeValueAsString(data);
        System.out.println(jsonArray);
        ;

        //stream.print();
        env.execute();
    }
}
