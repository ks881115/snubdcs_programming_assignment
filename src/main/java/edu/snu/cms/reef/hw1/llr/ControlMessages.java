/**
 * Copyright (C) 2014 Microsoft Corporation
 */
package edu.snu.cms.reef.hw1.llr;

import java.io.Serializable;

public enum ControlMessages implements Serializable {
  Compute,
  Stop,
  Evaluate
}