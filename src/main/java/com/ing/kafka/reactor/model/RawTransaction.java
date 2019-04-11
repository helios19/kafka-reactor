package com.ing.kafka.reactor.model;


import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

@Data
public class RawTransaction extends SpecificRecordBase {

    private String msg;
    private String name;

    @Override
    public Schema getSchema() {
        return null;
    }

    @Override
    public Object get(int field) {
        return null;
    }

    @Override
    public void put(int field, Object value) {

    }
}
