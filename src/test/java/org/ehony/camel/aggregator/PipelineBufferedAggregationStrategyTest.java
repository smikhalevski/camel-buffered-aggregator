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

public class PipelineBufferedAggregationStrategyTest extends CamelTestSupport
{

    @Produce(uri = "direct:start")
    private ProducerTemplate start;
    @EndpointInject(uri = "mock:kittyProcessor")
    private MockEndpoint mock;

    @Test
    public void testHappyPath() {
        // Returning "${body} Kitty" as a reply.
        mock.whenAnyExchangeReceived(new Processor()
        {

            @Override
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setBody(exchange.getIn().getBody() + " Kitty");
            }
        });
        assertEquals("Hello Doggy; Hello Kitty", start.send(createExchangeWithBody("Hello")).getIn().getBody());
    }

    @Test
    public void testPipelineThrowsException() throws Exception {
        // Throwing an exception as a reply.
        mock.whenAnyExchangeReceived(new Processor()
        {

            @Override
            public void process(Exchange exchange) throws Exception {
                throw new Exception("Expected exception while processing pipeline.");
            }
        });
        assertEquals("Hello Doggy; Kitty failed!", start.send(createExchangeWithBody("Hello")).getIn().getBody());
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
                        // Detecting weather kitty exchange failed.
                        String pipeKittyBody = exchanges.lookup("pipe-kitty").getIn().getBody(String.class);
                        if (exchanges.lookup("pipe-kitty").isFailed()) {
                            pipeKittyBody = "Kitty failed!";
                        }
                        return createExchangeWithBody(exchanges.lookup("pipe-doggy").getIn().getBody(String.class) + "; " + pipeKittyBody);
                    }
                };
                strategy.setCamelContext(getContext());

                from("direct:start")
                        .multicast(strategy).parallelProcessing()
                            .pipeline().id("pipe-kitty")
                                .to("mock:kittyProcessor")
                            .end()
                            .pipeline().id("pipe-doggy")
                                .transform(simple("${body} Doggy"))
                            .end()
                        .end();
            }
        };
    }
}