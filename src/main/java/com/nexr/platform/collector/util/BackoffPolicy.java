package com.nexr.platform.collector.util;



public interface BackoffPolicy {
  /**
   * Modify state as if a backoff had just happened. Call this after failed
   * attempts.
   */
  public void backoff();

  /**
   * Has time progressed enough to do a retry attempt?
   */
  public boolean isRetryOk();

  /**
   * Has so much time passed that we assume the failure is irrecoverable?
   */
  public boolean isFailed();

  /**
   * Reset backoff state. Call this after successful attempts.
   */
  public void reset();

  /**
   * Wait time in millis until RetryOk should be true
   */
  public long sleepIncrement();

  /**
   * Wait until it's ok to retry.
   */
  public void waitUntilRetryOk() throws InterruptedException;
}