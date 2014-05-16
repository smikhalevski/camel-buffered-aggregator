/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel.aggregator;

import org.apache.camel.*;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.model.*;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

import static org.apache.camel.Exchange.*;
import static org.apache.camel.model.ProcessorDefinitionHelper.*;
import static org.apache.commons.lang.StringUtils.isBlank;

public class BufferedAggregationStrategy implements AggregationStrategy, CamelContextAware, Aggregator<Sequence>
{

    private final static String COMBINER = "combiner";
    private CamelContext camelContext;
    private Aggregator<Sequence> aggregator;

    @Override
    public CamelContext getCamelContext() {
        return camelContext;
    }

    @Override
    public void setCamelContext(CamelContext context) {
        this.camelContext = context;
    }

    public Aggregator<Sequence> getAggregator() {
        if (aggregator == null) {
            aggregator = new DefaultAggregator();
        }
        return aggregator;
    }

    public void setAggregator(Aggregator<Sequence> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Exchange aggregate(Exchange a, Exchange b) {
        if (a == null) {
            a = new DefaultExchange(b.getContext(), ExchangePattern.InOnly);
            if (b.getProperties().containsKey(SPLIT_INDEX)) {
                a.setProperty(COMBINER, new Combinator(SPLIT_INDEX, SPLIT_COMPLETE));
            } else if (b.getProperties().containsKey(MULTICAST_INDEX)) {
                a.setProperty(COMBINER, new Combinator(MULTICAST_INDEX, MULTICAST_COMPLETE));
            }
            a.getIn().setBody(new ArrayList());
        }
        List<Exchange> list = a.getIn().getBody(List.class);
        Combinator combinator = a.getProperty(COMBINER, Combinator.class);
        combinator.merge(list, b);
        if (combinator.isReady(list)) {
            return aggregate(new DefaultSequence(list));
        }
        a.getIn().setBody(list);
        return a;
    }

    @Override
    public Exchange aggregate(Sequence exchanges) {
        return getAggregator().aggregate(exchanges);
    }

    public class DefaultAggregator implements Aggregator<Sequence>
    {

        @Override
        public Exchange aggregate(Sequence exchanges) {
            Exchange exchange = new DefaultExchange(getCamelContext());
            exchange.getOut().setBody(exchanges);
            return exchange;
        }
    }

    public class DefaultSequence extends ArrayList<Exchange> implements Sequence
    {

        public DefaultSequence(Collection<? extends Exchange> collection) {
            super(collection);
        }

        @Override
        public Exchange lookup(String name) {
            if (isBlank(name)) {
                throw new IllegalArgumentException("Pipeline name expected.");
            }
            int index = -1;
            boolean match = false;
            PipelineDefinition pipeline = null;
            for (RouteDefinition route : getCamelContext().getRouteDefinitions()) {
                Iterator<PipelineDefinition> it = filterTypeInOutputs(route.getOutputs(), PipelineDefinition.class);
                while (it.hasNext()) {
                    PipelineDefinition output = it.next();
                    if (name.equals(output.getId()) && isParentOfType(MulticastDefinition.class, output, false)) {
                        index = output.getParent().getOutputs().indexOf(output);
                        if (index >= 0) {
                            if (match) {
                                throw new IllegalStateException("Ambiguous pipeline name: " + name);
                            }
                            match = true;
                            pipeline = output;
                        }
                    }
                }
            }
            if (index < 0) {
                throw new IllegalArgumentException("Pipeline not found in context: " + name);
            }
            return get(index);
// TODO Potential defect.
//            Exchange exchange = get(index);
//            for (RouteNode node : exchange.getUnitOfWork().getTracedRouteNodes().getNodes()) {
//                if (pipeline.equals(node.getProcessorDefinition())) {
//                    return exchange;
//                }
//            }
//            return null;
        }
    }

    private class Combinator
    {

        private String indexPropertyName, readyPropertyName;

        public Combinator(String indexPropertyName, String readyPropertyName) {
            this.indexPropertyName = indexPropertyName;
            this.readyPropertyName = readyPropertyName;
        }

        public boolean isReady(List<Exchange> exchanges) {
            return CollectionUtils.exists(exchanges, new org.apache.commons.collections.Predicate()
            {

                @Override
                public boolean evaluate(Object object) {
                    return object instanceof Exchange && Boolean.TRUE.equals(((Exchange) object).getProperty(readyPropertyName));
                }
            });
        }

        public void merge(List<Exchange> exchanges, Exchange exchange) {
            if (exchange == null) {
                exchanges.add(null);
            } else {
                int index = exchange.getProperty(indexPropertyName, Integer.class),
                    size = exchanges.size() - 1;
                if (index > size) {
                    exchanges.addAll(Collections.nCopies(index - size, (Exchange) null));
                }
                exchanges.set(index, exchange);
            }
        }
    }
}