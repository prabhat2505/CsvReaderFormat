package org.example.pojo;


import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonPropertyOrder({"name","email"})
public class Employee {
    //public int id;
    public String name;
    public String email;
    Employee(){

    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }

    public Employee(String name, String email){
        this.name=name;
        this.email=email;
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        //return "name:"+this.name+"email:"+this.email;
    }
}
