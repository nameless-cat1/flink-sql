package hkust.Q10;

import hkust.Node;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Q10NationProcessFunction extends KeyedProcessFunction<Object, Node, Node> {

    String next_key="NATIONKEY";

    ValueState<Set<Node>> alive ;


    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Set<Node>> setDescriptor = TypeInformation.of(new TypeHint<Set<Node>>() {});
        ValueStateDescriptor<Set<Node>> descriptor = new ValueStateDescriptor<>("nation alive", setDescriptor);
        alive = getRuntimeContext().getState(descriptor);

    }

    public boolean isValue(Node node){
        return true;
    }

    @Override
    public void processElement(Node node, KeyedProcessFunction<Object, Node, Node>.Context context, Collector<Node> collector) throws Exception {
        if (alive.value() == null){
            alive.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }

        if (isValue(node)){
            Node tmp = new Node("Tmp", node.key, node.attribute_value, node.attribute_name);

            Set<Node> set = alive.value();
            if ("Insert".equals(node.type)){
                if (set.add(tmp)){
                    node.type="SetLive";
                    node.setKey(next_key);
                    collector.collect(node);

                }
            }
            else{
                if (set.contains(tmp)){
                    node.type="SetDead";
                    node.setKey(next_key);
                    collector.collect(node);
                    set.remove(tmp);
                }
            }
        }

    }
}
