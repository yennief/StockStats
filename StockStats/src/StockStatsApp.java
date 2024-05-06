import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.max;
import static java.util.Collections.min;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class StockStatsApp {

    static public class InputMetadata {
        public String Symbol;
        public String SecurityName;

    }

    static public class AggregationData {

        public String Stock;

        public String name;
        public double minLow;
        public double maxHigh;
        public double sumVolume;
        public double averageClose;

        public List<Double> closeValues = new ArrayList<>();

        public double getAverageClose() {
            double sum = closeValues.stream().mapToDouble(Double::doubleValue).sum();
            return sum / closeValues.size();
        }

    }

    static public class AggregationDataFinal {

        public String Stock;
        public String name;
        public double averageClose;
        public double minLow;
        public double maxHigh;
        public double sumVolume;


    }

    static public class InputScores {

        public String Stock;
        public String Date;
        public String Open;
        public String High;
        public String Low;
        public String Close;
        public String Volume;

    }
    static public class StockAlerts {

        public String stock;
        public String start_ts;
        public String end_ts;
        public double min_score;
        public double max_score;
        public double difference;
    }
    static public class StockStats {

        public String stock;
        public double min_score;
        public double max_score;
        public double difference;
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        if (args.length < 4) {
            System.out.println("Four parameters are required: boostrapServer windowTime difference delay<1 for mode A or 2 for mode C>");
            System.exit(0);
        }
        if(Integer.parseInt(args[3]) != 1 && Integer.parseInt(args[3]) != 2) {
            System.out.println("Delay must be either 1 for mode A, or 2 for mode C");
            System.exit(0);
        }
        final String boostrapServer = args[0];

        final int D = Integer.parseInt(args[1]);
        final int P = Integer.parseInt(args[2]);
        final int DELAY = Integer.parseInt(args[3]);

        Class.forName("com.mysql.cj.jdbc.Driver");

        final String MYSQL_URL = "jdbc:mysql://localhost:6033/kafkadatabase";
        final String TABLE_NAME = "aggData";
        final String MYSQL_USERNAME = "streamuser";
        final String MYSQL_PASSWORD = "stream";


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "house-alerts-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<InputScores> inputScoresSerde = new JsonPOJOSerde<>(InputScores.class);
        final Serde<InputMetadata> inputMetadataSerde = new JsonPOJOSerde<>(InputMetadata.class);
        final Serde<AggregationData> aggregationDataSerde = new JsonPOJOSerde<>(AggregationData.class);
        final Serde<AggregationDataFinal> aggregationDataFinalSerde = new JsonPOJOSerde<>(AggregationDataFinal.class);
        final Serde<StockStats> stockStatsSerde = new JsonPOJOSerde<>(StockStats.class);
        final Serde<StockAlerts> stockAlertsSerde = new JsonPOJOSerde<>(StockAlerts.class);
        StreamsBuilder builder = new StreamsBuilder();

        //dane załadowane do tematu kafki
        KStream<String, InputScores> data = builder.stream("kafka-input", Consumed.with(Serdes.String(), inputScoresSerde));

        KStream<String, InputMetadata> metadata = builder.stream("kafka-input-metadata", Consumed.with(Serdes.String(), inputMetadataSerde));

        //agregacja
        if(DELAY == 1) {
            KTable<Windowed<String>, AggregationData> aggData = data
                    .groupByKey()
                    .windowedBy(TimeWindows.of(Duration.ofDays(30))).

                    aggregate(
                            () -> {
                                AggregationData aggregatedData = new AggregationData();
                                aggregatedData.Stock = "";
                                aggregatedData.name = "";
                                aggregatedData.closeValues = new ArrayList<>();
                                aggregatedData.minLow = Long.MAX_VALUE;
                                aggregatedData.maxHigh = 0;
                                aggregatedData.sumVolume = 0;

                                return aggregatedData;
                            },
                            (aggKey, newValue, aggValue) -> {
                                aggValue.Stock = newValue.Stock;
                                aggValue.closeValues.add(Double.parseDouble(newValue.Close));
                                aggValue.minLow = Math.min(aggValue.minLow, Double.parseDouble(newValue.Low));
                                aggValue.maxHigh = Math.max(aggValue.maxHigh, Double.parseDouble(newValue.High));
                                aggValue.sumVolume += Double.parseDouble(newValue.Volume);
                                return aggValue;
                            },
                            Materialized.with(Serdes.String(), aggregationDataSerde)
                    );

            //finalny wynik agregacji, sprowadzony do streamu
            KStream<String, AggregationDataFinal> aggResult = aggData.toStream()

                    .map(
                            (key, value) -> {
                                AggregationDataFinal aggrData = new AggregationDataFinal();
                                aggrData.Stock = value.Stock;
                                aggrData.averageClose = value.getAverageClose();
                                aggrData.minLow = value.minLow;
                                aggrData.maxHigh = value.maxHigh;
                                aggrData.sumVolume = value.sumVolume;
                                return KeyValue.pair(key.key(), aggrData);

                            }
                    );

            aggResult.foreach((key, value) -> {
                try (Connection connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD)) {
                    PreparedStatement statement = connection
                            .prepareStatement("INSERT INTO " + TABLE_NAME + "(key_col, average_close, min_low, max_high, sum_volume) VALUES (?, ?, ?, ?, ?)");
                    statement.setString(1, key);
                    statement.setDouble(2, value.averageClose);
                    statement.setDouble(3, value.minLow);
                    statement.setDouble(4, value.maxHigh);
                    statement.setDouble(5, value.sumVolume);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        }
//        }
      if(DELAY == 2) {

            KTable<Windowed<String>, AggregationData> aggData  = data
                    .groupByKey()
                    .windowedBy(TimeWindows.of(Duration.ofDays(30))).

                    aggregate(
                            () -> {
                                AggregationData aggregatedData = new AggregationData();
                                aggregatedData.Stock = "";
                                aggregatedData.name = "";
                                aggregatedData.closeValues = new ArrayList<>();
                                aggregatedData.minLow = Long.MAX_VALUE;
                                aggregatedData.maxHigh = 0;
                                aggregatedData.sumVolume = 0;

                                return aggregatedData;
                            },
                            (aggKey, newValue, aggValue) -> {
                                aggValue.Stock = newValue.Stock;
                                aggValue.closeValues.add(Double.parseDouble(newValue.Close));
                                aggValue.minLow = Math.min(aggValue.minLow, Double.parseDouble(newValue.Low));
                                aggValue.maxHigh = Math.max(aggValue.maxHigh, Double.parseDouble(newValue.High));
                                aggValue.sumVolume += Double.parseDouble(newValue.Volume);
                                return aggValue;
                            },
                            Materialized.with(Serdes.String(), aggregationDataSerde)
                    ).suppress(Suppressed.untilWindowCloses(unbounded()));

            //finalny wynik agregacji, sprowadzony do streamu
            KStream<String, AggregationDataFinal> aggResult = aggData.toStream()

                    .map(
                            (key, value) -> {
                                AggregationDataFinal aggrData = new AggregationDataFinal();
                                aggrData.Stock = value.Stock;
                                aggrData.averageClose = value.getAverageClose();
                                aggrData.minLow = value.minLow;
                                aggrData.maxHigh = value.maxHigh;
                                aggrData.sumVolume = value.sumVolume;
                                return KeyValue.pair(key.key(), aggrData);

                            }
                    );

            aggResult.foreach((key, value) -> {
                try (Connection connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD)) {
                    PreparedStatement statement = connection
                            .prepareStatement("INSERT INTO " + TABLE_NAME + "(key_col, average_close, min_low, max_high, sum_volume) VALUES (?, ?, ?, ?, ?)");
                    statement.setString(1, key);
                    statement.setDouble(2, value.averageClose);
                    statement.setDouble(3, value.minLow);
                    statement.setDouble(4, value.maxHigh);
                    statement.setDouble(5, value.sumVolume);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        }

//        aggResult.to("aggregation-output", Produced.with(Serdes.String(),aggregationDataFinalSerde));

        //wykrywanie anomalii
        KTable<Windowed<String>, StockStats> stats = data
                .groupByKey().
                windowedBy(TimeWindows.of(Duration.ofDays(D))).
                aggregate(
                        () -> {
                            StockStats hs = new StockStats();
                            hs.stock = "";
                            hs.min_score = Long.MAX_VALUE;
                            hs.max_score = 0;
                            hs.difference = 0;
                            return hs;
                        },
                        (aggKey, newValue, aggValue) -> {
                            aggValue.stock = newValue.Stock;
                            aggValue.min_score = Math.min(aggValue.min_score, Double.parseDouble(newValue.Low));
                            aggValue.max_score = Math.max(aggValue.max_score, Double.parseDouble(newValue.High));
                            aggValue.difference = (Double.parseDouble(newValue.High) - Double.parseDouble(newValue.Low)) * 100 / Double.parseDouble(newValue.High);
                            return aggValue;
                        },
                        Materialized.with(Serdes.String(), stockStatsSerde)
                );

        KStream<String, StockAlerts> resultStream = stats.toStream()
                .filter((key, value) -> {
                    return value.difference > P;
                })
                .map(
                        (key, value) -> {
                            StockAlerts ha = new StockAlerts();
                            ha.start_ts = key.window().startTime().toString();
                            ha.end_ts = key.window().endTime().toString();
                            ha.stock = value.stock;
                            ha.min_score = value.min_score;
                            ha.max_score = value.max_score;
                            ha.difference = value.difference;
                            return KeyValue.pair(key.key(), ha);

                        }
                );

        resultStream.to("kafka-output", Produced.with(Serdes.String(), stockAlertsSerde));


        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch latch = new CountDownLatch(1);
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });
            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
