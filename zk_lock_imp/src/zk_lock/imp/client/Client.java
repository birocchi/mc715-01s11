package zk_lock.imp.client;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.recipes.lock.WriteLock;

public class Client extends Thread implements Watcher
{
	private ZooKeeper zk;
	private volatile boolean dead;	
	
	/* properties */
	
	public boolean getDead()
	{
		return dead;
	}

	/* constructor */
	
	public Client(String[] hosts, int port) throws IOException
	{
		StringBuilder connString = new StringBuilder();
		
		for(String h : hosts)
			connString.append(h + ":").append(port).append(",");
		
		connString.setLength(connString.length() - 1);		
		
		dead = false;		
		zk = new ZooKeeper(connString.toString(), 3000, this);		
	}
		
	@Override
	public void process(WatchedEvent event)
	{	
		if (event.getType() == Event.EventType.None)
		{
			// estado da conexao mudou
			switch(event.getState())
			{
				case SyncConnected:
					// conectamos
					// podemos iniciar					
					break;
					
				case Expired:
					// sessao expirada
					dead = true;
					break;
			}
		}
		else
		{
			// algo no nó mudou
		}
	}
	
	/**
	 * Starts this client.
	 */
	public void run()
	{				
		BufferedReader stdin = new BufferedReader(new java.io.InputStreamReader(System.in));
		String line;
		
		System.out.println("Esperando conexão...");
				
		try
		{
			while (zk.getState() != States.CONNECTED)
			sleep(500);
		} 
		catch (InterruptedException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println("Conexão estabelescida.");
		
		try
		{
			while ((line = stdin.readLine().toLowerCase()) != "quit")
			{
				String[] args = line.split(" "); 
				String cmd = args[0];
				
				if (args.length < 2)
				{
					System.out.println("Usage:");
					System.out.println("[lock | unlock] /lockname");
				}
				else
				{
					String arg = args[1];
					
					if (arg.charAt(0) != '/')
						System.out.println("Lock deve começar com /");
					else					
					{
						if (cmd.equals("lock"))
						{					
							lock(arg);
						}
						else if (cmd.equals("unlock"))
						{
							unlock(arg);
						}	
					}
				}
			}
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
//		try
//		{
//			
//		} 
//		catch (KeeperException e)
//		{
//			e.printStackTrace();
//		} 
//		catch (InterruptedException e)
//		{
//			e.printStackTrace();
//		}
	}

	private void unlock(String arg)
	{
		WriteLock lock = new WriteLock(zk, arg, null);
		
		System.out.println("Liberando lock " + arg + "...");
		lock.unlock();
		System.out.println("Lock " + arg + " liberado");
	}

	private void lock(String arg)
	{
		WriteLock lock = new WriteLock(zk, arg, null);
		
		if (lock.isOwner())
			System.out.println("Você já possui o lock " + arg);
		else
		{
			try
			{
				System.out.println("Obtendo lock " + arg + "...");
				
				if (lock.lock())
					System.out.println("Lock " + arg + " obtido com sucesso");
				else
					System.out.println("Lock " + arg + " não foi obtido");
			} 
			catch (KeeperException e)
			{
				e.printStackTrace();
			} 
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
}
