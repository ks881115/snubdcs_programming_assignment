/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package edu.snu.cms.reef.hw1.llr;

import java.util.Iterator;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

import javax.inject.Inject;

/**
 *
 */
public class IntegerSumReduceFunction implements
		ReduceFunction<Integer> {

	@Inject
	public IntegerSumReduceFunction() {
	}

	@Override
	public Integer apply(Iterable<Integer> elements) {
		// TODO Auto-generated method stub
		int sum = 0;
		for(Integer num : elements) {
			sum += num.intValue();
		}
		return new Integer(sum);
	}

}
