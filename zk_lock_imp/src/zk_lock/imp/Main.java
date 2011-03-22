package zk_lock.imp;

import org.apache.zookeeper.recipes.lock.WriteLock;

public class Main
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		WriteLock wl = new WriteLock(null, "a", null);
	}
}
