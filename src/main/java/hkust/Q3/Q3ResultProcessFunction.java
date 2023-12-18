package hkust.Q3;

import hkust.Node;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Q3ResultProcessFunction extends KeyedProcessFunction<Object, Node, Node> {

    ValueState<Double>old_value;

    List<String>output_name= Arrays.asList("ORDERKEY","O_ORDERDATE","O_SHIPPRIORITY");

    String aggregateName = "revenue";

    String nextKey="ORDERKEY";

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Double> old_value_descriptor = new ValueStateDescriptor<Double>("old value",Double.class);
        old_value = getRuntimeContext().getState(old_value_descriptor);

    }

    @Override
    public void processElement(Node node, KeyedProcessFunction<Object, Node, Node>.Context context, Collector<Node> collector) throws Exception {

        if (old_value.value()==null){
            old_value.update(0.0);
        }

        Double delta = getAggregate(node);
        double new_value=0.0;
        if (node.type.equals("Add")){
            new_value=old_value.value()+delta;
        } else if (node.type.equals("Sub")) {
            new_value=old_value.value()-delta;
        }
        old_value.update(new_value);
        List<Object>attribute_value=new ArrayList<>();
        List<String>attribute_name=new ArrayList<>();
        for (String s : output_name) {
            attribute_value.add(node.getValueByName(s));
            attribute_name.add(s);
        }
        attribute_value.add(new_value);
        attribute_name.add(aggregateName);
        node.attribute_value=attribute_value;
        node.attribute_name=attribute_name;
        node.type="Output";
        node.setKey(nextKey);
        collector.collect(node);

    }

    public Double getAggregate(Node node){
        return (Double)node.getValueByName("L_EXTENDEDPRICE")*(1.0-(Double)node.getValueByName("L_DISCOUNT"));
    }

}
