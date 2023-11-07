package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.example.pojo.Employee;


public class CsvReadFileToPojo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CsvReaderFormat<Employee> csvReaderFormat = CsvReaderFormat.forPojo(Employee.class);
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
