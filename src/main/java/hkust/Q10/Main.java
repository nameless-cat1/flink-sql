package hkust.Q10;

import hkust.Node;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class Main {

    static OutputTag<Node> lineitemTag = new OutputTag<Node>("lineitem"){};
    static OutputTag<Node> orderTag = new OutputTag<Node>("order"){};
    static OutputTag<Node> customerTag = new OutputTag<Node>("customer"){};
    static OutputTag<Node> nationTag = new OutputTag<Node>("nation"){};


    public static void main(String[] args) throws Exception {
//        String input_path = "./input_data/input_data_0_001G.csv";
//        String output_path = "./input_data/output_data_0_001G.csv";
//        String path = args[0];
//        String input_path = "file:///home/nameless-cat/flink-data/input_data_0_001G.csv";
//        String output_path = "file:///home/nameless-cat/flink-data/output_data_0_001G.csv";

        String input_path = args[0];
        String output_path = args[1];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<String> data = env.readTextFile(input_path).setParallelism(1);

        SingleOutputStreamOperator<Node> original_stream = getOriginalData(data);

        DataStream<Node> lineitem = original_stream.getSideOutput(lineitemTag);
        DataStream<Node> order = original_stream.getSideOutput(orderTag);
        DataStream<Node> customer = original_stream.getSideOutput(customerTag);
        DataStream<Node> nation = original_stream.getSideOutput(nationTag);

        DataStream<Node> nationResult = nation.keyBy(node -> node.key).process(new Q10NationProcessFunction());
//        nationResult.print();
        DataStream<Node> customerResult = nationResult.connect(customer).keyBy(node -> node.key, node -> node.key).process(new Q10CustomerProcessFunction());
//        customerResult.print();
        DataStream<Node> orderResult = customerResult.connect(order).keyBy(node -> node.key, node -> node.key).process(new Q10OrderProcessFunction());
//        orderResult.print();
        DataStream<Node> lineitemResult = orderResult.connect(lineitem).keyBy(node -> node.key, node -> node.key).process(new Q10LineitemProcessFunction());
        DataStreamSink<Node> result = lineitemResult.keyBy(node -> node.key).process(new Q10ResultProcessFunction()).writeAsText(output_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

//        DataStreamSink<Node> output = result.writeAsText(output_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);



//        env.setParallelism(1);
        env.execute();
    }


    static SingleOutputStreamOperator<Node> getOriginalData(DataStreamSource<String> data){
        return data.process(new ProcessFunction<String, Node>() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            @Override
            public void processElement(String s, ProcessFunction<String, Node>.Context context, Collector<Node> collector) throws Exception {
                String header = s.substring(0, 3);
                String[] element = s.substring(3).split("\\|");
//                System.out.println(element[0]);

                switch (header){
                    case "+LI":
                        context.output(lineitemTag,
                                new Node("Insert", Long.valueOf(element[0]),
                                        new ArrayList<>(Arrays.asList(Integer.valueOf(element[3]), Long.valueOf(element[0]), Double.valueOf(element[5]),element[8],element[15], Double.valueOf(element[6]))),
                                        new ArrayList<>(Arrays.asList("LINENUMBER","ORDERKEY","L_EXTENDEDPRICE","L_RETURNFLAG","L_COMMENT","L_DISCOUNT"))));
                        break;
                    case "-LT":
                        context.output(lineitemTag,
                                new Node("Delete", Long.valueOf(element[0]),
                                        new ArrayList<>(Arrays.asList(Integer.valueOf(element[3]), Long.valueOf(element[0]), Double.valueOf(element[5]),element[8],element[15], Double.valueOf(element[6]))),
                                        new ArrayList<>(Arrays.asList("LINENUMBER","ORDERKEY","L_EXTENDEDPRICE","L_RETURNFLAG","L_COMMENT","L_DISCOUNT"))));
                        break;
                    case "+OR":
                        context.output(orderTag,
                                new Node("Insert", Long.valueOf(element[1]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[1]), Long.valueOf(element[0]), format.parse(element[4]),element[8])),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","ORDERKEY","O_ORDERDATE","O_COMMENT"))));
                        break;
                    case "-OR":
                        context.output(orderTag,
                                new Node("Delete", Long.valueOf(element[1]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[1]), Long.valueOf(element[0]), format.parse(element[4]),element[8])),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","ORDERKEY","O_ORDERDATE","O_COMMENT"))));
                        break;
                    case "+CU":
                        context.output(customerTag,
                                new Node("Insert", Long.valueOf(element[3]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[0]), Long.valueOf(element[3]),
                                                element[1],Double.valueOf(element[5]), element[4],element[2],element[7])),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","NATIONKEY","C_NAME","C_ACCTBAL","C_PHONE","C_ADDRESS","C_COMMENT"))));
                        break;
                    case "-CU":
                        context.output(customerTag,
                                new Node("Delete", Long.valueOf(element[3]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[0]), Long.valueOf(element[3]),
                                                element[1],Double.valueOf(element[5]), element[4],element[2],element[7])),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","NATIONKEY","C_NAME","C_ACCTBAL","C_PHONE","C_ADDRESS","C_COMMENT"))));
                        break;
                    case "+NA":
                        context.output(nationTag,
                                new Node("Insert", Long.valueOf(element[0]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[0]), element[1],element[3])),
                                        new ArrayList<>(Arrays.asList("NATIONKEY","N_NAME","N_COMMENT"))));
                        break;
                    case "-NA":
                        context.output(nationTag,
                                new Node("Delete", Long.valueOf(element[0]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[0]), element[1],element[3])),
                                        new ArrayList<>(Arrays.asList("NATIONKEY","N_NAME","N_COMMENT"))));
                        break;

                }
            }
        }).setParallelism(1);
    }

}



