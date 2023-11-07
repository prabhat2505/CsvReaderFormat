package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.example.pojo.Employee;




// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class CsvReadFileToPojo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CsvReaderFormat<Employee> csvReaderFormat = CsvReaderFormat.forPojo(Employee.class);
        String path = "src\\main\\resources";
        final FileSource<Employee> source =
                FileSource.forRecordStreamFormat(csvReaderFormat,new Path(path))
                        .build();

        final DataStream<Employee> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        try {
            CloseableIterator<Employee> employeeList = stream.executeAndCollect();
            System.out.println(employeeList.next());
            while(employeeList.hasNext()) {
                System.out.println(employeeList);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        stream.print();
        env.execute();
    }
}