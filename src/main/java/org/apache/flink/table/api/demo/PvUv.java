package org.apache.flink.table.api.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.python.CustomConnectorDescriptor;
import org.apache.flink.table.functions.ScalarFunction;

public class PvUv {

    public static class Multi extends ScalarFunction {
        public Long eval(Long param) {
            return param * 10;
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.LONG;
        }

        @Override
        public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
            return new TypeInformation[]{Types.LONG};
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        EnvironmentSettings settings = new EnvironmentSettings.Builder()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.connect(new Kafka()
                .version("0.11")
                .topic("user_behavior")
                .startFromEarliest()
                .property("zookeeper.connect", "11.132.139.43:2181")
                .property("bootstrap.servers", "11.132.139.43:9092"))
                .withFormat(new Json()
                        .failOnMissingField(true)
                        .jsonSchema("{" +
                                "  type: 'object'," +
                                "  properties: {" +
                                "    user_id: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    item_id: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    category_id: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    behavior: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    ts: {" +
                                "      type: 'string'," +
                                "      format: 'date-time'" +
                                "    }" +
                                "  }" +
                                "}"
                        ))
                .withSchema(new Schema()
                        .field("user_id", Types.STRING)
                        .field("item_id", Types.STRING)
                        .field("category_id", Types.STRING)
                        .field("behavior", Types.STRING)
                        .field("rowtime", Types.SQL_TIMESTAMP)
                        .rowtime(new Rowtime()
                                .timestampsFromField("ts")
                                .watermarksPeriodicBounded(60000)))
                .inAppendMode()
                .registerTableSource("source");

        CustomConnectorDescriptor customConnector = new CustomConnectorDescriptor("jdbc", 1, false);
        customConnector.property("connector.driver", "org.apache.derby.jdbc.ClientDriver");
        customConnector.property("connector.url", "jdbc:derby://11.132.139.43:1527/firstdb");
        customConnector.property("connector.table", "pv_uv_table");
        customConnector.property("connector.write.flush.max-rows", "1");
        tEnv.connect(customConnector)
                .withSchema(new Schema()
                        .field("startTime", Types.SQL_TIMESTAMP)
                        .field("endTime", Types.SQL_TIMESTAMP)
                        .field("pv", Types.LONG)
                        .field("uv", Types.LONG))
                .registerTableSink("sink");

        tEnv.registerFunction("multi", new Multi());

        tEnv.scan("source")
                .window(Tumble.over("1.hours").on("rowtime").as("w"))
                .groupBy("w")
                .select("w.start as startTime, w.end as endTime, COUNT(1) as pv, user_id.count.distinct as uv")
                .select("startTime, endTime, multi(pv), uv")
                .insertInto("sink");

        tEnv.execute("table pv uv java");
    }


}
