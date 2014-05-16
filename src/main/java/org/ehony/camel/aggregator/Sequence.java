/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel.aggregator;

import java.util.List;

import org.apache.camel.Exchange;

public interface Sequence extends List<Exchange>
{

    Exchange lookup(String name);
}
