import hkust.Node;
import hkust.Q3.Q3CustomerProcessFunction;
import hkust.Q3.Q3LineitemProcessFunction;
import hkust.Q3.Q3OrderProcessFunction;
import hkust.Q3.Q3ResultProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class TestQ3_2 {

    static OutputTag<Node> lineitemTag = new OutputTag<Node>("lineitem"){};
    static OutputTag<Node> orderTag = new OutputTag<Node>("order"){};
    static OutputTag<Node> customerTag = new OutputTag<Node>("customer"){};


    public static void main(String[] args) throws Exception {
        String input_path = "./input_data/input_data_0_01G.csv";
        String output_path = "./input_data/output_data_0_01G.csv";
//        String path = args[0];
//        String input_path = "file:///home/nameless-cat/flink-data/input_data_0_001G.csv";
//        String output_path = "file:///home/nameless-cat/flink-data/output_data_0_001G.csv";

//        String input_path = args[0];
//        String output_path = args[1];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> data = env.readTextFile(input_path).setParallelism(1);

        SingleOutputStreamOperator<Node> original_stream = getOriginalData(data);

        DataStream<Node> lineitem = original_stream.getSideOutput(lineitemTag);
        DataStream<Node> order = original_stream.getSideOutput(orderTag);
        DataStream<Node> customer = original_stream.getSideOutput(customerTag);

        DataStream<Node> customerResult = customer.keyBy(node -> node.key).process(new Q3CustomerProcessFunction());
//        customerResult.print();

        DataStream<Node> orderResult = customerResult.connect(order).keyBy(node -> node.key, node -> node.key).process(new Q3OrderProcessFunction());

        DataStream<Node> lineitemResult = orderResult.connect(lineitem).keyBy(node -> node.key, node -> node.key).process(new Q3LineitemProcessFunction());
//        lineitemResult.print();

        DataStream<Node> result = lineitemResult.keyBy(node -> node.key).process(new Q3ResultProcessFunction());
        result.print();
//        DataStreamSink<Node> output = result.writeAsText(output_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);



//        env.setParallelism(1);
        env.execute();
    }


    static SingleOutputStreamOperator<Node> getOriginalData(DataStreamSource<String> data){
        return data.process(new ProcessFunction<String, Node>() {
            long count = 0;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            @Override
            public void processElement(String s, ProcessFunction<String, Node>.Context context, Collector<Node> collector) throws Exception {
                String header = s.substring(0, 3);
                String[] element = s.substring(3).split("\\|");
//                System.out.println(element[0]);

                switch (header){
                    case "+LI":
                        count++;
                        context.output(lineitemTag,
                                new Node("Insert", Long.valueOf(element[0]),
                                        new ArrayList<>(Arrays.asList(format.parse(element[10]), Integer.valueOf(element[3]), Long.valueOf(element[0]), Double.valueOf(element[5]), Double.valueOf(element[6]))),
                                        new ArrayList<>(Arrays.asList("L_SHIPDATE", "LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT"))));
                        break;
//                    case "-LT":
//                        count++;
//                        context.output(lineitemTag,
//                                new Node("Delete", Long.valueOf(element[0]),
//                                        new ArrayList<>(Arrays.asList(format.parse(element[10]), Integer.valueOf(element[3]), Long.valueOf(element[0]), Double.valueOf(element[5]), Double.valueOf(element[6]))),
//                                        new ArrayList<>(Arrays.asList("L_SHIPDATE", "LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT"))));
//                        break;
                    case "+OR":
                        count++;
                        context.output(orderTag,
                                new Node("Insert", Long.valueOf(element[1]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[1]), Long.valueOf(element[0]), format.parse(element[4]),Integer.valueOf(element[7]))),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","ORDERKEY","O_ORDERDATE","O_SHIPPRIORITY"))));
                        break;
                    case "-OR":
                        if (Long.valueOf(element[0])>2000) {
                            count++;
                            context.output(orderTag,
                                    new Node("Delete", Long.valueOf(element[1]),
                                            new ArrayList<>(Arrays.asList(Long.valueOf(element[1]), Long.valueOf(element[0]), format.parse(element[4]), Integer.valueOf(element[7]))),
                                            new ArrayList<>(Arrays.asList("CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"))));
                        }
                        break;
                    case "+CU":
                        count++;
                        context.output(customerTag,
                                new Node("Insert", Long.valueOf(element[0]),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(element[0]), element[6])),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","C_MKTSEGMENT"))));
                        break;
                    case "-CU":
                        if (Long.valueOf(element[0])>100) {
                            count++;
                            context.output(customerTag,
                                    new Node("Delete", Long.valueOf(element[0]),
                                            new ArrayList<>(Arrays.asList(Long.valueOf(element[0]), element[6])),
                                            new ArrayList<>(Arrays.asList("CUSTKEY", "C_MKTSEGMENT"))));
                        }
                        break;
                }
            }
        }).setParallelism(1);
    }

}



