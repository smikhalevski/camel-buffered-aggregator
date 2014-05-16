/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel.aggregator;

import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

import static java.util.Arrays.asList;

public class SplitBufferedAggregationStrategyTest extends CamelTestSupport
{

    @Produce(uri = "direct:start")
    private ProducerTemplate start;
    @EndpointInject(uri = "mock:bodyProcessor")
    private MockEndpoint mock;


    @Test
    public void testHappyPath() {
        // Returning "${body};" as a reply.
        mock.whenAnyExchangeReceived(new Processor()
        {

            @Override
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setBody(exchange.getIn().getBody() + ";");
            }
        });
        assertEquals("Apples;Peaches;Bananas;", start.send(createExchangeWithBody(asList("Apples", "Peaches", "Bananas"))).getIn().getBody());
    }

    @Test
    public void testIterationThrowsException() throws Exception {
        // Throwing an exception on "Bananas" exchange.
        mock.whenAnyExchangeReceived(new Processor()
        {

            @Override
            public void process(Exchange exchange) throws Exception {
                String body = exchange.getIn().getBody(String.class);
                if ("Bananas".equals(body)) {
                    throw new Exception("Expected exception while processing iteration.");
                }
                exchange.getIn().setBody(body + ";");
            }
        });
        assertEquals("Apples;Peaches;Failed to process Bananas exchange!", start.send(createExchangeWithBody(asList("Apples", "Peaches", "Bananas"))).getIn().getBody());
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder()
        {

            @Override
            public void configure() {
                BufferedAggregationStrategy strategy = new BufferedAggregationStrategy()
                {

                    @Override
                    public Exchange aggregate(Sequence exchanges) {
                        StringBuilder answer = new StringBuilder();
                        for (Exchange exchange : exchanges)
                            if (exchange.isFailed()) {
                                // Showing specific message for failed exchanges.
                                answer.append("Failed to process " + exchange.getIn().getBody() + " exchange!");
                            } else {
                                answer.append(exchange.getIn().getBody());
                            }
                        return createExchangeWithBody(answer.toString());
                    }
                };
                strategy.setCamelContext(getContext());

                from("direct:start")
                        .split(body(), strategy).parallelProcessing()
                            .to("mock:bodyProcessor");
            }
        };
    }
}
