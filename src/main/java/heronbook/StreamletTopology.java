package heronbook;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.heron.api.utils.Utils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.KeyedWindow;
import org.apache.heron.streamlet.KVStreamlet;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;

public final class StreamletTopology {
  static Logger logger = Logger.getLogger(StreamletTopology.class.getName());

  public static void main(String[] args) throws Exception {
    Builder builder = Builder.newBuilder();

    //////////////////// spouts
    // `supplier` emitting single tuple
    Streamlet<Character> s1 = builder
      .newSource(() ->
        (char)ThreadLocalRandom.current().nextInt('A', 'D'))
      .setName("SerializableSupplier-ABCD");
    s1.setNumPartitions(1);
    // use `source` to do `supplier`
    Streamlet<Character> s1_ = builder
      .newSource(new Source<Character>() {
        @Override
        public void setup(Context context) {}

        @Override
        public Collection<Character> get() {
          return Arrays.asList(
            (char)ThreadLocalRandom.current().nextInt('A', 'D'));
        }

        @Override
        public void cleanup() {}
      })
      .setName("Source-ABCD");
    s1_.setNumPartitions(1);
    // powerful `source` emitting batch tuples
    Streamlet<Character> s2 = builder
      .newSource(new Source<Character>() {
        @Override
        public void setup(Context context) {}

        @Override
        public Collection<Character> get() {
          return ThreadLocalRandom.current().ints(7, 'C', 'F')
            .mapToObj(x -> (char)x).collect(Collectors.toList());
        }

        @Override
        public void cleanup() {}
      })
      .setName("Source-CDEF");
    s2.setNumPartitions(1);

    ///////////////////////// sinks
    s1.log();
    s1.consume(tuple -> System.out.println(String.valueOf(tuple)));
    s1.toSink(new Sink<Character>() {
      @Override
      public void setup(Context context) {}

      @Override
      public void put(Character tuple) {
        System.out.println(String.valueOf(tuple));
      }

      @Override
      public void cleanup() {}
    });

    s1_.log();
    s2.log();

    ////////////////////////// filter, map, flatmap, transform
    // filter
    Streamlet<Character> f1 =
      s1.filter(x -> x.equals('A') || x.equals('C'));
    f1.setName("filter-AC").log();
    // transform doing filter
    Streamlet<Character> t1 = s1.transform(
        new SerializableTransformer<Character, Character>() {
      @Override
      public void setup(Context context) {}

      @Override
      public void transform(Character x,
                            Consumer<Character> consumer) {
        if (x.equals('A') || x.equals('C')) {
          consumer.accept(x);
        }
      }

      @Override
      public void cleanup() {}
    });
    t1.setName("transform-filter").log();
    // map
    Streamlet<Boolean> m1 =
      f1.map(x -> x.equals('A') ? true : false);
    m1.setName("map-boolean").log();
    // transform as map
    Streamlet<Boolean> t2 = t1.transform(
        new SerializableTransformer<Character, Boolean>() {
      @Override
      public void setup(Context context) {}

      @Override
      public void transform(Character x,
                            Consumer<Boolean> consumer) {
        if (x.equals('A')) {
          consumer.accept(true);
        } else {
          consumer.accept(false);
        }
      }

      @Override
      public void cleanup() {}
    });
    t2.setName("transform-map").log();
    // flatmap
    Streamlet<Character> fm1 = m1.flatMap(
        x -> x ? Arrays.asList('A', 'B') : Arrays.asList('C', 'D'));
    fm1.setName("flatMap-ABCD").log();
    // transform as flatmap
    Streamlet<Character> t3 = t2.transform(
        new SerializableTransformer<Boolean, Character>() {
      @Override
      public void setup(Context context) {}

      @Override
      public void transform(Boolean x,
                            Consumer<Character> consumer) {
        if (x) {
          consumer.accept('A');
          consumer.accept('B');
        } else {
          consumer.accept('C');
          consumer.accept('D');
        }
      }

      @Override
      public void cleanup() {}
    });
    t3.setName("transform-flatmap").log();

    ///////////////////////// chain style
    s1.filter(x -> x.equals('A') || x.equals('C'))
      .map(x -> x.equals('A') ? true : false)
      .flatMap(x -> x ? Arrays.asList('A', 'B')
                      : Arrays.asList('C', 'D'));

    ///////////////////////// repartition
    s1.repartition(3, (c, i) -> {
      logger.info("i="+i);
      return Arrays.asList(c%2);
    }).setName("repartition").log();

    /////////////////////// clone, union
    List<Streamlet<Character>> c1 = s1.clone(2);
    for (int i=0; i<2; i++) {
      c1.get(i).setName("clone"+i).log();
    }
    s1.union(s2).setName("union").log();

    ///////////////////////// reduce
    KVStreamlet<KeyedWindow<Character>, Integer> r1 = s1
      .reduceByKeyAndWindow(x -> x, x -> 1, 
        WindowConfig.TumblingCountWindow(5), 
        (x, y) -> x + y); 
    r1.setName("reduceByKeyAndWindow-TumblingCountWindow").log();

    KVStreamlet<KeyedWindow<Character>, Integer> r2 = s2
      .reduceByKeyAndWindow(x -> x, 
        WindowConfig.SlidingTimeWindow(
          Duration.ofSeconds(3), Duration.ofSeconds(1)), 
        0, (x, y) -> x + 1); 
    r2.setName("reduceByKeyAndWindow-SlidingTimeWindow").log();

    ///////////////////////// join
    Streamlet<KeyValue<KeyedWindow<Character>, Integer>> j1 = r1
        .join(r2, x -> x.getKey().getKey(), y -> y.getKey().getKey(),
            WindowConfig.TumblingCountWindow(11), JoinType.INNER, 
            (x, y) -> x.getValue() + y.getValue());
    j1.setName("join-INNER").log();
    KVStreamlet<KeyedWindow<Character>, Integer> j2 = r1.join(
      r2, x -> x.getKey().getKey(), y -> y.getKey().getKey(), 
      WindowConfig.TumblingCountWindow(11), JoinType.OUTER_LEFT, 
      (x, y) -> (x==null ? 0 : x.getValue()) +
                (y==null ? 0 : y.getValue())); 
    j2.setName("join-OUTER_LEFT").log();
    Streamlet<KeyValue<KeyedWindow<Character>, Integer>> j3 = r1
        .join(r2, x -> x.getKey().getKey(), y -> y.getKey().getKey(),
            WindowConfig.TumblingCountWindow(11), JoinType.OUTER_RIGHT, 
            (x, y) -> (x!=null?x.getValue():0) + (y!=null?y.getValue():0));
    j3.setName("join-OUTER_RIGHT").log();
    Streamlet<KeyValue<KeyedWindow<Character>, Integer>> j4 = r1
        .join(r2, x -> x.getKey().getKey(), y -> y.getKey().getKey(),
            WindowConfig.TumblingCountWindow(11), JoinType.OUTER, 
            (x, y) -> (x!=null?x.getValue():0) + (y!=null?y.getValue():0));
    j4.setName("join-OUTER").log();

    //////// config
    Config config = Config.newBuilder()
        .setNumContainers(1)
        .build();

    new Runner().run(args[0], config, builder);
  }
}