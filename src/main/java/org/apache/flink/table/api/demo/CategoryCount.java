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
import org.apache.flink.table.functions.ScalarFunction;

public class CategoryCount {

    public static class Shard extends ScalarFunction {
        public Long eval(String param) {
            return Long.valueOf(param) % 10;
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.LONG;
        }

        @Override
        public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
            return new TypeInformation[]{Types.STRING};
        }
    }

    public static class Identity extends ScalarFunction {
        public Long eval(Long param) {
            return param;
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
        env.setParallelism(5);

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

        tEnv.connect(new Kafka()
                .version("0.11")
                .topic("java_sales_volume_table")
                .property("zookeeper.connect", "11.132.139.43:2181")
                .property("bootstrap.servers", "11.132.139.43:9092"))
                .withFormat(new Json()
                        .failOnMissingField(true)
                        .jsonSchema("{" +
                                "  type: 'object'," +
                                "  properties: {" +
                                "    startTime: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    endTime: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    category_id: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    sales_volume: {" +
                                "      type: 'string'" +
                                "    }" +
                                "  }" +
                                "}"
                        ))
                .withSchema(new Schema()
                        .field("startTime", Types.STRING)
                        .field("endTime", Types.STRING)
                        .field("category_id", Types.STRING)
                        .field("sales_volume", Types.STRING))
                .inAppendMode()
                .registerTableSink("sink");

        tEnv.registerFunction("shard", new Shard());
        tEnv.registerFunction("identity", new Identity());

        tEnv.scan("source")
                .addOrReplaceColumns("identity(identity(shard(category_id) + 1) -1) as category_id")
                .window(Tumble.over("1.hours").on("rowtime").as("w"))
                .groupBy("w, category_id")
                .select("w.start.cast(STRING) as startTime, w.end.cast(STRING) as endTime, " +
                        "category_id.cast(STRING) as category_id, COUNT(1).cast(STRING) as sales_volume")
                .insertInto("sink");

        tEnv.execute("category count java demo");
    }
}
