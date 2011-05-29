package zk_tpcp.imp;

import java.io.IOException;

import zk_tpcp.imp.client.Client;

public class Main
{
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		if (args.length < 2)
		{
			System.out.println("Usage: zk_lock_imp <zk_host> <port>");
			System.exit(1);
		}
		
		String host = args[0];
		int port;
		Client c;	
		
		try
		{
			port = Integer.parseInt(args[1]);
			c = new Client(new String[] { host } , port);

			c.start();
			
			// esperamos cliente morrer
			while (!c.getDead())
				Thread.sleep(1000);
			
			System.exit(0);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Porta inválida.");
		}
		catch (IOException e)
		{
			e.printStackTrace();			
		} 
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
				
		System.exit(2);
	}
}
