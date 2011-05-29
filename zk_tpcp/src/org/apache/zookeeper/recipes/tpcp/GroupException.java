package org.apache.zookeeper.recipes.tpcp;

/**
 * Exception occurring during group managing.
 *
 */
public class GroupException extends Exception 
{
	/**
	 */
	private static final long serialVersionUID = 1L;

	public GroupException(String message)
	{
		super(message);
	}
}
