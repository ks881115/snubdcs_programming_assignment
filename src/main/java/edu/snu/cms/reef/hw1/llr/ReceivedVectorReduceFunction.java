/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package edu.snu.cms.reef.hw1.llr;

import java.util.Iterator;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;

import javax.inject.Inject;

/**
 *
 */
public class ReceivedVectorReduceFunction implements
		ReduceFunction<DenseVector> {

	@Inject
	public ReceivedVectorReduceFunction() {
	}

	@Override
	public DenseVector apply(Iterable<DenseVector> elements) {
		// TODO Auto-generated method stub
		Iterator<DenseVector> itr = elements.iterator();
		DenseVector reducedVector = new DenseVector(itr.next());
		
		while (itr.hasNext()) {
			reducedVector.add(itr.next());
		}
		return reducedVector;
	}

}
