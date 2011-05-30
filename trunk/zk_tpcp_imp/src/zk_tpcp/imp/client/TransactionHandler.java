package zk_tpcp.imp.client;

import java.io.Serializable;

import org.apache.zookeeper.recipes.tpcp.ITransactionHandler;

public class TransactionHandler implements ITransactionHandler
{

	@Override
	public boolean execute(Serializable arg0)
	{
		System.out.println("Opa, transacao");
		
		return false;
	}

}
