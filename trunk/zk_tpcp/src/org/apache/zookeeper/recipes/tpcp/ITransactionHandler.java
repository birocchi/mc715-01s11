package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;

/**
 * Handler for executing transactions.
 */
public interface ITransactionHandler 
{
	/**
	 * Callback method for transaction execution.
	 * @param query query to be executed
	 * @return true to commit, false to abort
	 */
	public boolean execute(Serializable query);
}
