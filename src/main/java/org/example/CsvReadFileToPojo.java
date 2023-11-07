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


// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class CsvReadFileToPojo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /////////////////////////////////////////// Test Code 1/////////////////////
//        SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = mapper ->
//                mapper.schemaFor(Employee.class).withoutQuoteChar().withColumnSeparator(',');
//
//        CsvReaderFormat<Employee> csvReaderFormat =
//                CsvReaderFormat.forSchema(CsvMapper::new, schemaGenerator, TypeInformation.of(Employee.class));
        /////////////////////////////////////////////////////////////////////


        //SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = mapper ->
                //mapper.schemaFor(Employee.class).withoutQuoteChar().withColumnSeparator(',');

       //CsvReaderFormat<Employee> csvReaderFormat =
                //CsvReaderFormat.forSchema(CsvMapper::new, schemaGenerator, (TypeInformation<Employee>) TypeInformation.of(Employee.class));

        CsvReaderFormat<Employee> csvReaderFormat = CsvReaderFormat.forPojo(Employee.class);
        //FileSource<Employee> source =
                //FileSource.forRecordStreamFormat(csvReaderFormat, Path.fromLocalFile(new File("src/main/resources/employee.csv"))).build();

        String path = "src\\main\\resources";
        final FileSource<Employee> source =
                FileSource.forRecordStreamFormat(csvReaderFormat,new Path(path))
                        .build();

        final DataStream<Employee> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
//        stream.map(S).executeAndCollect();
        try {
              CloseableIterator<Employee> employeeList = stream.executeAndCollect();
            System.out.println(employeeList.next());
            while(employeeList.hasNext()) {
                System.out.println(employeeList);
            }
//            if(closeableIterator.hasNext()) {
//                System.out.println(closeableIterator.getClass().getName());
//            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

//       KeyedStream<Employee,String> keyedStream = stream.keyBy(employee->employee.name);
        stream.print();
        env.execute();
    }
}