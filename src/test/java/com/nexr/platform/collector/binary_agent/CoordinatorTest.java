package com.nexr.platform.collector.binary_agent;

import java.util.Properties;

import com.nexr.distributed.ConfigurationException;
import com.nexr.distributed.Coordinator;
import com.nexr.distributed.CoordinatorService;
import com.nexr.distributed.Service;
import com.nexr.distributed.ServiceFactory;
import com.nexr.distributed.ServiceUnavailableException;
import com.nexr.distributed.group.GroupMembership;
import com.nexr.distributed.group.Member;
import com.nexr.distributed.group.MemberAlreadyExistsException;

public class CoordinatorTest {
	public static void main(String args[]) {
		Properties props = new Properties();
		props.setProperty("com.nexr.zk.servers", "localhost:2181");

		Coordinator membershipCoordinator = null;
		
		Member member = null;
		try {
			CoordinatorService membershipService = (CoordinatorService) ServiceFactory
					.createService(Service.Type.Coordinator, props);
			membershipService.start();
			membershipCoordinator = membershipService.createCoordinator("NexR");
			
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (ServiceUnavailableException e) {
			e.printStackTrace();
		}
		GroupMembership myGMS = membershipCoordinator
				.createGroupMembership("MyGMS");
		
		
		
		myGMS.leaveForcely("agent1");
		try {
			member = myGMS.join("agent1", "Start");
		} catch (MemberAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		member.writeProfile("Stop");
		member.leave();
	}
}
