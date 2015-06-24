package storm.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

import java.util.*;

/**
 * Created by sumitc on 4/29/15.
 */
public class TridentStateExample  {

    public enum PartyName {
        Democratic,
        Republican,
    }

    public static class RandomVoteSpout implements IBatchSpout {
        Random _rand;
        Integer _constituencyId;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        }

        public void open(Map map, TopologyContext topologyContext) {
            _rand = new Random();
            _constituencyId = topologyContext.getThisTaskIndex();
        }

        public void emitBatch(long batchId, TridentCollector collector) {
            Utils.sleep(1000);
            for(Integer count = 0; count< 100; count++){
                PartyName[] partyNames = PartyName.values();
                collector.emit(new Values(this._constituencyId, partyNames[_rand.nextInt(partyNames.length)].toString()));
            }
        }

        public void ack(long l) {

        }

        public void close() {

        }

        public Map getComponentConfiguration() {
            return null;
        }

        public Fields getOutputFields() {
            return new Fields("ConstituencyId", "PartyName");
        }
    }

    public static class ConstituencyState {
       Long DemocratsVotes = 0L;
       Long RepublicanVotes = 0L;
    }

    public static class VoteCountAggregator extends BaseAggregator<VoteCountAggregator.CountState> {
        private static final Logger LOG = Logger.getLogger(VoteCountAggregator.class);
        private Object _batchId;
        static class CountState {
            Integer constituencyId;
            ConstituencyState state = new ConstituencyState();
        }

        public CountState init(Object batchId, TridentCollector collector) {
            this._batchId = batchId;
            CountState state = new CountState();
            return state;
        }

        public void aggregate(CountState state, TridentTuple tuple, TridentCollector collector) {
            Integer constituencyId = tuple.getIntegerByField("ConstituencyId");
            state.constituencyId = constituencyId;
            PartyName partyName = PartyName.valueOf(tuple.getStringByField("PartyName"));
            LOG.debug(String.format("Got tuple %s %s %s", constituencyId, partyName, state.state.DemocratsVotes));
            if (partyName == PartyName.Democratic) {
                state.state.DemocratsVotes++;
            } else if (partyName == partyName.Republican) {
                state.state.RepublicanVotes++;
            }
        }

        public void complete(CountState state, TridentCollector collector) {
            LOG.debug(String.format("Emitting counts for Constituency %d - Democracts %d Republican %d for Batch %s",
                    state.constituencyId, state.state.DemocratsVotes, state.state.RepublicanVotes, _batchId));
            collector.emit(new Values(state.state));
        }
    }

    public static class WordSplit extends BaseFunction {
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static class ElectionState implements State {
        private static final Logger LOG = Logger.getLogger(ElectionState.class);

        private final Map<Integer, ConstituencyState> totalCount;

        public ElectionState() {
            totalCount = new HashMap<Integer, ConstituencyState>();
        }


        public void setStateValues(List<Integer> constituencyList, List<ConstituencyState> batchStates) {
            for(int i=0; i<constituencyList.size(); i++) {
                Integer constituencyId = constituencyList.get(i);
                ConstituencyState batchUpdateForConstituency = batchStates.get(i);
                if (!totalCount.containsKey(constituencyId)){
                   totalCount.put(constituencyId, new ConstituencyState());
                }

                ConstituencyState totalCountsForConstituency = totalCount.get(constituencyId);
                totalCountsForConstituency.DemocratsVotes += batchUpdateForConstituency.DemocratsVotes;
                totalCountsForConstituency.RepublicanVotes+= batchUpdateForConstituency.RepublicanVotes;
                LOG.debug(String.format("Commited counts for Constituency %d - Current Counts - Democrats %d Republican %d ",
                        constituencyId, totalCountsForConstituency.DemocratsVotes, totalCountsForConstituency.RepublicanVotes));
            }
        }

        public List<ConstituencyState> getCurrentCountsForConstituency(List<Integer> constituencyList)
        {
            List<ConstituencyState> states = new ArrayList<ConstituencyState>();
            for(int i=0; i<constituencyList.size(); i++) {
                Integer constituencyId = constituencyList.get(i);
                LOG.debug(String.format("GET for %d", constituencyId));
                if (totalCount.containsKey(constituencyId)) {
                    states.add(totalCount.get(constituencyId));
                } else {
                    states.add(new ConstituencyState());
                }
            }
            return states;
        }

        public List<ConstituencyState> getAllCounts()
        {
            return new ArrayList<ConstituencyState>(totalCount.values());
        }

        public void beginCommit(Long aLong) {

        }

        public void commit(Long aLong) {

        }
    }


    public static class StateUpdater extends BaseStateUpdater<ElectionState> {
        public void updateState(ElectionState state, List<TridentTuple> tuples, TridentCollector collector) {
            List<Integer> ids = new ArrayList<Integer>();
            List<ConstituencyState> states = new ArrayList<ConstituencyState>();
            for(TridentTuple t: tuples) {
                //System.out.println(t);
                ids.add(t.getInteger(0));
                states.add((ConstituencyState) t.get(1));
            }
            state.setStateValues(ids, states);
        }
    }

    public static class StateUpdaterFactory implements StateFactory {
        public storm.trident.state.State makeState(Map conf, IMetricsContext metricsContext, int partitionIndex, int numPartitions) {
            return new ElectionState();
        }
    }

    public static class QueryConstituencyLeader
            extends BaseQueryFunction<ElectionState, Object> {
        public List<Object> batchRetrieve(ElectionState state, List<TridentTuple> inputs) {
            List<Integer> ids = new ArrayList<Integer>();
            List<Object> result = new ArrayList<Object>();
            for(TridentTuple input: inputs) {
                ids.add(Integer.valueOf(input.getString(0)));
            }
            List<ConstituencyState> currentStates = state.getCurrentCountsForConstituency(ids);
            for(ConstituencyState constituencyState : currentStates ) {
                if (constituencyState.DemocratsVotes > constituencyState.RepublicanVotes) {
                    result.add(PartyName.Democratic);
                } else {
                    result.add(PartyName.Republican);
                }
            }
            return result;
        }

        public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }

    public static class QueryConstituencyCount
            extends BaseQueryFunction<ElectionState, Object> {
        public List<Object> batchRetrieve(ElectionState state, List<TridentTuple> inputs) {
            List<Integer> ids = new ArrayList<Integer>();
            List<Object> result = new ArrayList<Object>();
            for(TridentTuple input: inputs) {
                ids.add(Integer.valueOf(input.getString(0)));
            }
            List<ConstituencyState> currentStates = state.getCurrentCountsForConstituency(ids);
            for(ConstituencyState constituencyState : currentStates ) {
                HashMap<String,Long> counts = new HashMap<String, Long>();
                counts.put(PartyName.Democratic.toString(),constituencyState.DemocratsVotes);
                counts.put(PartyName.Republican.toString(),constituencyState.RepublicanVotes);
                result.add(counts);
            }
            return result;
        }

        public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }

    public static class QueryElectionLeader
            extends BaseQueryFunction<ElectionState, Object> {
        public List<Object> batchRetrieve(ElectionState state, List<TridentTuple> inputs) {
            List<Integer> ids = new ArrayList<Integer>();
            List<Object> result = new ArrayList<Object>();
            List<ConstituencyState> currentStates = state.getAllCounts();
            Long democratsLeadingCount = 0L;
            Long republicansLeadingCount = 0L;
            for(ConstituencyState constituencyState : currentStates ) {
                if (constituencyState.DemocratsVotes > constituencyState.RepublicanVotes) {
                    democratsLeadingCount++;
                } else {
                    republicansLeadingCount++;
                }
            }
            Map<String, Long> map = new HashMap<String, Long>();
            map.put(PartyName.Democratic.toString(), democratsLeadingCount);
            map.put(PartyName.Republican.toString(), republicansLeadingCount);
            result.add(map);
            return result;
        }

        public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        TridentState state = topology.newStaticState(new StateUpdaterFactory());
        TridentState loginCounts =
                topology.newStream("voting-events", new RandomVoteSpout())
                        .name("VoteStream")
                        .parallelismHint(100)
                        .groupBy(new Fields("ConstituencyId"))
                        .aggregate(new Fields("PartyName", "ConstituencyId"), new VoteCountAggregator(), new Fields("count"))
                        .name("CountAggregator")
                        .partitionPersist(new StateUpdaterFactory(), new Fields("ConstituencyId", "count"), new StateUpdater())
                        .parallelismHint(1); // Single Partition Persist

        LocalDRPC drpc = new LocalDRPC();

        topology.newDRPCStream("count", drpc)
                .stateQuery(loginCounts, new Fields("args"), new QueryConstituencyCount(), new Fields("count"))
                .each(new Fields("count"), new FilterNull());

        topology.newDRPCStream("leader", drpc)
                .stateQuery(loginCounts, new Fields("args"), new QueryElectionLeader(), new Fields("count"))
                .each(new Fields("count"), new FilterNull());

        LocalCluster cluster = new LocalCluster();


        Config conf = new Config();
        cluster.submitTopology("trident", conf,
                topology.build());

        for (int i = 0; i < 1000; i++) {
            System.out.println(String.format("Current Counts for Constituency %s", drpc.execute("count", "1")));
            System.out.println(String.format("Current Election Counts %s", drpc.execute("leader", "")));
            Utils.sleep(2000);
        }
        cluster.shutdown();
        drpc.shutdown();
    }
}
