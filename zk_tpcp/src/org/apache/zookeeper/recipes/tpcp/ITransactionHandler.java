package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;

/**
 * Handler for executing transactions.
 */
public interface ITransactionHandler {
    /**
     * Callback method for transaction execution.
     * @param transactionID transaction unique identifier
     * @param query query to be executed
     * @return true to commit, false to abort
     */
    public boolean execute(long transactionID, Serializable query);

    /**
     * Callback method for transaction result.
     * @param transactionID transaction unique identifier
     * @param result true to final commit, false to abort
     */
    public void result(long transactionID, boolean result);
}
