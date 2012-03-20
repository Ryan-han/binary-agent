/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nexr.platform.collector.util;

import static java.lang.Math.max;

import java.util.Map;

public class ExponentialBackoff implements BackoffPolicy {
	final long initialSleep;
	final long maxTries;

	long backoffCount = 0; // number of consecutive backoff calls;
	long sleepIncrement; // amount of time before retry should happen
	long retryTime; // actual unixtime to compare against when retry is ok.

	public ExponentialBackoff(long initialSleep, long max) {
		this.initialSleep = initialSleep;
		this.maxTries = max;
		reset();
	}

	/**
	 * Modify state as if a backoff had just happened. Call this after failed
	 * attempts.
	 */
	public void backoff() {
		retryTime = Clock.unixTime() + sleepIncrement;
//		sleepIncrement *= 2;
		backoffCount++;
	}

	/**
	 * Has time progressed enough to do a retry attempt?
	 */
	public boolean isRetryOk() {
		return retryTime <= Clock.unixTime() && !isFailed();
	}

	/**
	 * Has so much time passed that we assume the failure is irrecoverable?
	 * 
	 * If this becomes true, it will never return true on isRetryOk, until this
	 * has been reset.
	 */
	public boolean isFailed() {
		return backoffCount >= maxTries;
	}

	/**
	 * Reset backoff state. Call this after successful attempts.
	 */
	public void reset() {
		sleepIncrement = initialSleep;
		long cur = Clock.unixTime();
		retryTime = cur;
		backoffCount = 0;
	}

	public long sleepIncrement() {
		return sleepIncrement;
	}

	public String getName() {
		return "ExpBackoff";
	}

	
	/**
	 * Sleep until we reach retryTime. Call isRetryOk after this returns if you
	 * are concerned about backoff() being called while this thread sleeps.
	 */
	public void waitUntilRetryOk() throws InterruptedException {
		Thread.sleep(max(0L, retryTime - Clock.unixTime()));
	}
}
