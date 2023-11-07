package org.example;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.example.pojo.Employee;

public class MySumReducer implements ReduceFunction<Employee> {
    @Override
    public Employee reduce(Employee argo,Employee arg1) throws Exception {
        return new Employee(argo.name, argo.email);
    }
}
