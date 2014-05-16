/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel.aggregator;

import org.apache.camel.Exchange;

public interface Aggregator<T extends Iterable<Exchange>>
{

    Exchange aggregate(T exchanges);
}
