package org.apache.flink.table.api.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class CategoryCountBatch {
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
        EnvironmentSettings settings = new EnvironmentSettings.Builder()
                .inBatchMode()
                .useBlinkPlanner()
                .build();
        TableEnvironmentImpl tEnv = (TableEnvironmentImpl) TableEnvironment.create(settings);
        ((PlannerBase) tEnv.getPlanner()).getExecEnv().setParallelism(5);

        tEnv.registerFunction("shard", new Shard());
        tEnv.registerFunction("identity", new Identity());

        String source_file = "hdfs:///user/duanchen/user_behavior.csv";
        tEnv.registerTableSource("source",
                new CsvTableSource(source_file,
                        new String[]{"user_id", "item_id", "category_id", "behavior", "rowtime"},
                        new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                Types.SQL_TIMESTAMP}));

        String result_file = "hdfs:///user/duanchen/batch_java_sales_volume_table.csv";

        CsvTableSink sink = new CsvTableSink(result_file, ",");
        TableSink<Row> csvSink = sink.configure(new String[]{"startTime", "endTime", "category_id", "sales_volume"},
                new TypeInformation[]{Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG, Types.LONG});
        tEnv.registerTableSink("sink", csvSink);

        tEnv.scan("source")
                .addOrReplaceColumns("identity(identity(shard(category_id) + 1) -1) as category_id")
                .window(Tumble.over("1.hours").on("rowtime").as("w"))
                .groupBy("w, category_id")
                .select("w.start as startTime, w.end as endTime, category_id as category_id, " +
                        "COUNT(1) as sales_volume")
                .insertInto("sink");

        tEnv.execute("category count batch java demo");
    }
}
