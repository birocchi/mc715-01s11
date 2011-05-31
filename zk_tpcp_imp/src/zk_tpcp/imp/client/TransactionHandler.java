package zk_tpcp.imp.client;

import java.io.IOException;
import java.io.Serializable;

import org.apache.zookeeper.recipes.tpcp.ITransactionHandler;

public class TransactionHandler implements ITransactionHandler
{
	@Override
	public boolean execute(long transactionID, Serializable query)
	{
		boolean commit = false;
		
		synchronized (System.in)
		{
		
			System.out.println("Opa, chegou transacao. Query: " + query.toString());
			System.out.println("Deseja commitar [S\\n]: ");
			
			try
			{
				while (true)
				{
					char c = (char)System.in.read();
					
					if (c == '\n' || c == 's' || c == 'S')
					{
						commit = true;
						System.out.println("PreCommit transacao [id:"+transactionID+"] (query:'"+ query.toString() + "' )");
						break;
					}
					else if (c == 'n' || c == 'N')
					{
						commit = false;
						System.out.println("Abortando transacao [id:"+transactionID+"] (query:'"+ query.toString() + "' )");
						break;
					}					
				}
			} 
			catch (IOException e)
			{
				e.printStackTrace();
			}
			
		}
		
		return commit;
	}
	
	@Override
	public void result(long transactionID, boolean result)
	{
		synchronized (System.in)
		{
			System.out.println("Resultado transacao [id:"+transactionID+"]: " + (result ? "COMMIT" : "ABORT"));
		}
	}

}
