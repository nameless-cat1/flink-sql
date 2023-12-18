package hkust.Q10;

import hkust.Node;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Q10OrderProcessFunction extends KeyedCoProcessFunction<Object, Node, Node, Node> {

    String next_key="ORDERKEY";

    ValueState<Integer>count;


    ValueState<Set<Node>> tmp_node;

    // only store the attribute_name and attribute_value of alive node
    ValueState<Node> alive_node;


    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> count_descriptor = new ValueStateDescriptor<Integer>("order alive number",Integer.class);
        count =getRuntimeContext().getState(count_descriptor);

        TypeInformation<Set<Node>> tmp_node_descriptor = TypeInformation.of(new TypeHint<Set<Node>>() {});
        ValueStateDescriptor<Set<Node>> set_descriptor = new ValueStateDescriptor<>("order tmp node", tmp_node_descriptor);
        tmp_node = getRuntimeContext().getState(set_descriptor);

        ValueStateDescriptor<Node> alive_node_descriptor = new ValueStateDescriptor<Node>("alive node", Node.class);
        alive_node = getRuntimeContext().getState(alive_node_descriptor);

    }

    @Override
    public void processElement1(Node node, KeyedCoProcessFunction<Object, Node, Node, Node>.Context context, Collector<Node> collector) throws Exception {
        if (tmp_node.value()==null){
            tmp_node.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            count.update(0);
            alive_node.update(null);
        }

        if (node.type.equals("SetLive")){
            Integer number = count.value()+1;
            count.update(number);
            alive_node.update(new Node(node));
            Set<Node> nodes = tmp_node.value();
            for (Node tmp_node : nodes) {
                output_node(node,tmp_node,collector);
            }

        }
        else{
            int number = count.value() - 1;
            count.update(number);
            Set<Node> nodes = tmp_node.value();
            for (Node tmp_node : nodes) {
                output_node(node,tmp_node,collector);
            }

            alive_node.update(null);
        }

    }

    @Override
    public void processElement2(Node node, KeyedCoProcessFunction<Object, Node, Node, Node>.Context context, Collector<Node> collector) throws Exception {
        if (tmp_node.value()==null){
            tmp_node.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            count.update(0);
            alive_node.update(null);
        }

        Node store_node = new Node(node.attribute_value, node.attribute_name);
        if (isValid(node)) {
            if (node.type.equals("Insert")) {
                if (count.value() == 1) {
                    // to make sure insert comes before delete
                    if (tmp_node.value().add(store_node)) {
                        node.type = "SetLive";
                        output_node(node, alive_node.value(), collector);

                    }
                } else {
                    tmp_node.value().add(store_node);
                }
            } else if (node.type.equals("Delete")) {
                if (count.value() == 1) {
                    if (tmp_node.value().contains(store_node)) {
                        tmp_node.value().remove(store_node);
                        node.type = "SetDead";
                        output_node(node, alive_node.value(), collector);

                    }

                } else {
                    tmp_node.value().remove(store_node);
                }
            }
        }

    }

    public boolean isValid(Node node) throws ParseException {
//        return true;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        if (((Date) node.getValueByName("O_ORDERDATE")).compareTo(format.parse("1993-03-15"))>=0
                && ((Date) node.getValueByName("O_ORDERDATE")).compareTo(format.parse("1996-03-15"))<0){
            return true;
        }
        else {
            return false;
        }
    }


    // add additional attributes and output node to next process
    public void output_node(Node output_node, Node connect_node, Collector<Node> collector) throws IOException {
        Node node = new Node(output_node);

        if (connect_node!=null){
            for (int i=0;i<connect_node.attribute_name.size();i++){
                if (!node.attribute_name.contains(connect_node.attribute_name.get(i))){
                    node.attribute_name.add(connect_node.attribute_name.get(i));
                    node.attribute_value.add(connect_node.attribute_value.get(i));
                }
            }
        }

        node.setKey(next_key);
        collector.collect(node);

    }



}


