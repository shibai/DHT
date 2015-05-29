/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
    
    hasMyReplicas.clear();
    haveReplicasOf.clear();
    // check the ring in last round
    if (ring.size() > 0) {
        populateNeighborNodes();
    }
    
    // assign the current ring
    ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    if (ring.size() > 0 && ht->currentSize() > 0) stabilizationProtocol();
}

// update hasMyReplicas and haveReplicasOf
void MP2Node::populateNeighborNodes() {
    int curHash = hashFunction(memberNode->addr.addr);
    int i;
    for (i = 0; i < ring.size(); i++) {
        if (curHash == ring.at(i).getHashCode()) {
            break;
        }
    }
    
    hasMyReplicas.push_back(ring.at((i + 1) % ring.size()));
    hasMyReplicas.push_back(ring.at((i + 2) % ring.size()));
    
    haveReplicasOf.push_back( ring.at((i - 2 + ring.size()) % ring.size()) );
    haveReplicasOf.push_back(ring.at((i - 1 + ring.size()) % ring.size()));   
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    g_transID++;
    vector<Node> replicas = findNodes(key);
    Message msg(g_transID,memberNode->addr,CREATE,key,value,PRIMARY);
    quorum[g_transID] = vector<string>();
    outgoingMsgTimestamp[g_transID] = par->getcurrtime(); 
    outgoingMsg.emplace(g_transID,msg); // attention!!! opertor[] would result in compile-error 
       
    Message msg_sec(g_transID,memberNode->addr,CREATE,key,value,SECONDARY);
    Message msg_ter(g_transID,memberNode->addr,CREATE,key,value,TERTIARY);
    sendMsg(msg,&replicas[0].nodeAddress);
    sendMsg(msg_sec,&replicas[1].nodeAddress);
    sendMsg(msg_ter,&replicas[2].nodeAddress);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
    g_transID++;
    vector<Node> replicas = findNodes(key);
    Message msg(g_transID,memberNode->addr,READ,key);
    quorum[g_transID] = vector<string>();
    outgoingMsgTimestamp[g_transID] = par->getcurrtime(); 
   	outgoingMsg.emplace(g_transID,msg); // attention!!! opertor[] would result in compile-error 
       
    sendMsg(msg,&replicas[0].nodeAddress);
    sendMsg(msg,&replicas[1].nodeAddress);
    sendMsg(msg,&replicas[2].nodeAddress);

}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
    g_transID++;
    vector<Node> replicas = findNodes(key);
    Message msg(g_transID,memberNode->addr,UPDATE,key,value,PRIMARY);
    quorum[g_transID] = vector<string>();
    outgoingMsgTimestamp[g_transID] = par->getcurrtime(); 
   	outgoingMsg.emplace(g_transID,msg); // attention!!! opertor[] would result in compile-error 
       
    Message msg_sec(g_transID,memberNode->addr,UPDATE,key,value,SECONDARY);
    Message msg_ter(g_transID,memberNode->addr,UPDATE,key,value,TERTIARY);
    sendMsg(msg,&replicas[0].nodeAddress);
    sendMsg(msg_sec,&replicas[1].nodeAddress);
    sendMsg(msg_ter,&replicas[2].nodeAddress);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
    g_transID++;
    vector<Node> replicas = findNodes(key);
    Message msg(g_transID,memberNode->addr,DELETE,key);
    quorum[g_transID] = vector<string>();
    outgoingMsgTimestamp[g_transID] = par->getcurrtime(); 
   	outgoingMsg.emplace(g_transID,msg); // attention!!! opertor[] would result in compile-error 
    
    sendMsg(msg,&replicas[0].nodeAddress);
    sendMsg(msg,&replicas[1].nodeAddress);
    sendMsg(msg,&replicas[2].nodeAddress);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    Entry entry(value,par->getcurrtime(),replica); 
    return ht->create(key,entry.convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    Entry entry(value,par->getcurrtime(),replica); 
    return ht->update(key,entry.convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size - 1);
		/*
		 * Handle the message types here
		 */
        //message.pop_back(); // delete last space
        //message = message.substr(0,message.length() - 2);
        
        handleMsg(message);
	}
    
    // clean up
    // loop through all keys in quorum and check for time-outs    
    for (auto it : outgoingMsgTimestamp) {
        if (par->getcurrtime() - it.second > 10) {
			// get type of msg
            Message msg = outgoingMsg.at(it.first);
			if (static_cast<MessageType>(msg.type) == CREATE) {
				log->logCreateFail(&memberNode->addr,true,it.first,msg.key,msg.value);
            }else if (static_cast<MessageType>(msg.type) == READ) {
                log->logReadFail(&memberNode->addr,true,it.first,msg.key);
            }else if (static_cast<MessageType>(msg.type) == UPDATE) {
                log->logUpdateFail(&memberNode->addr,true,it.first,msg.key,msg.value);
            }else if (static_cast<MessageType>(msg.type) == DELETE) {
                log->logDeleteFail(&memberNode->addr,true,it.first,msg.key);
            }
            
            quorum.erase(it.first);
            outgoingMsg.erase(it.first);
            outgoingMsgTimestamp.erase(it.first);
        }
    }
    
}

// wrapper for all message types handling
void MP2Node::handleMsg(string message) {
    string originalMsg = message;
    int found = message.find("::");
    int transID = stoi(message.substr(0,found));
    message = message.substr(found + 2);
    
    found = message.find("::");
    string fromAddr = message.substr(0,found);
    message = message.substr(found + 2);
    
    found = message.find("::");
    MessageType msgType = static_cast<MessageType>(stoi(message.substr(0,found))); 
    message = message.substr(found + 2);

    if (msgType == CREATE || msgType == UPDATE) {
        createUpdateMsgHandler(message,transID,fromAddr,msgType);
    }else if (msgType == DELETE) {
        deleteMsgHandler(message,transID,fromAddr);
    }else if (msgType == READ) {
        readMsgHandler(message,transID,fromAddr);
    }else if (msgType == REPLY) {
        replyMsgHandler(originalMsg,message,transID);
    }else if (msgType == READREPLY) {
        readReplyMsgHandler(originalMsg,message,transID);
    }
}

// wrapper for message sending
void MP2Node::sendMsg(Message msg, Address *toAddr) {
    string msgStr = msg.toString();
    char* msgChar = (char*)malloc(msgStr.size() + 1);

    strcpy(msgChar,msgStr.c_str());
    emulNet->ENsend(&memberNode->addr,toAddr,msgChar,msgStr.size() + 1);
    
    free(msgChar);
}

// handles READREPLY messages
void MP2Node::readReplyMsgHandler(string originalMsg, string value, int transID) {
    quorum[transID].push_back(originalMsg);
    
    if (quorum[transID].size() >= 2) {
        Message outgoingMessage = outgoingMsg.at(transID);
        log->logReadSuccess(&memberNode->addr,true,transID,outgoingMessage.key,value);
        
        // close this transaction
        quorum.erase(transID);
        outgoingMsg.erase(transID);
        outgoingMsgTimestamp.erase(transID);
    }
}

// handles READ messages
void MP2Node::readMsgHandler(string key,int transID,string masterAddrStr) {
    Address masterAddr = Address(masterAddrStr);
    string value = readKey(key);
    
    if (value.length() != 0) {
        log->logReadSuccess(&memberNode->addr,false,transID,key,value);
    }else {
        log->logReadFail(&memberNode->addr,false,transID,key);
    }
    
    Message readReply(transID,memberNode->addr,value);
    sendMsg(readReply,&masterAddr);
}

// handles REPLY messages
void MP2Node::replyMsgHandler(string originalMsg, string leftMsg,int transID) {
    if (transID != 0) {
        quorum[transID].push_back(originalMsg);
    }
    
    // if all replies have been received
    if (quorum[transID].size() >= 2) {
        int vote = 0;
        
        for (int i = 0; i < quorum[transID].size(); i++) {
            if (leftMsg == "1") {
                vote++;
            }
        }
        
        Message outgoingMessage = outgoingMsg.at(transID);
        if (vote >= 2) {
            if (outgoingMessage.type == CREATE) {
                log->logCreateSuccess(&memberNode->addr,true,transID,outgoingMessage.key,outgoingMessage.value);
            }else if (outgoingMessage.type == UPDATE) {
                log->logUpdateSuccess(&memberNode->addr,true,transID,outgoingMessage.key,outgoingMessage.value); 
            }else if (outgoingMessage.type == DELETE) {
                log->logDeleteSuccess(&memberNode->addr,true,transID,outgoingMessage.key);
            }
        }else {
            if (outgoingMessage.type == CREATE) {
                log->logCreateFail(&memberNode->addr,true,transID,outgoingMessage.key,outgoingMessage.value);
            }else if (outgoingMessage.type == UPDATE) {
                log->logUpdateFail(&memberNode->addr,true,transID,outgoingMessage.key,outgoingMessage.value);
            }else if (outgoingMessage.type == DELETE) {
                log->logDeleteFail(&memberNode->addr,true,transID,outgoingMessage.key);
            }
        }
        
        // close this transaction
        quorum.erase(transID);
        outgoingMsg.erase(transID);
        outgoingMsgTimestamp.erase(transID);
    }
}

// handles DELETE msg
void MP2Node::deleteMsgHandler(string key, int transID, string masterAddrStr) {
    Address masterAddr = Address(masterAddrStr);
    bool success = deletekey(key);
    
    if (success) {
        log->logDeleteSuccess(&memberNode->addr,false,transID,key);
    }else {
        log->logDeleteFail(&memberNode->addr,false,transID,key);
    }
    
    Message reply(transID,memberNode->addr,REPLY,success);
    sendMsg(reply,&masterAddr);
}


// handles CREATE and UPDATE msg
void MP2Node::createUpdateMsgHandler(string message, int transID, string masterAddrStr, MessageType msgType) {
    int found = message.find("::");
    string key = message.substr(0,found);
    message = message.substr(found + 2);
            
    found = message.find("::");
    string value = message.substr(0,found);
            
    ReplicaType replicaType = static_cast<ReplicaType>(stoi(message.substr(found + 2)));
    Address masterAddr = Address(masterAddrStr);
    
    // create/update the K/V pair on local hash table and send back a reply to master
    bool success;
    if (msgType == CREATE) {
        success = createKeyValue(key,value, replicaType);
        
        // logging
        if (success) {
            log->logCreateSuccess(&memberNode->addr,false,transID,key,value);
        }else {
            log->logCreateFail(&memberNode->addr,false,transID,key,value);
        }
    }else if (msgType == UPDATE) {
        success = updateKeyValue(key,value, replicaType);
        
        // logging
        if (success) {
            log->logUpdateSuccess(&memberNode->addr,false,transID,key,value);
        }else {
            log->logUpdateFail(&memberNode->addr,false,transID,key,value);
        }
    }
    
    Message reply(transID,memberNode->addr,REPLY,success);
    sendMsg(reply,&masterAddr);
}


/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    int curHash = hashFunction(memberNode->addr.addr);
    int i;
    for (i = 0; i < ring.size(); i++) {
        if (curHash == ring.at(i).getHashCode()) {
            break;
        }
    }
    
    Node pre1 = ring.at( ((i - 2 + ring.size()) % ring.size()) );
    Node pre2 = ring.at( ((i - 1 + ring.size()) % ring.size()) );
    Node post1 = ring.at((i + 1) % ring.size());
    Node post2 = ring.at((i + 2) % ring.size());
    
    /*
     * Topology:   
     * pre1 - pre2 - currentNode - post1 - post2
     *
     */ 
     string a = "";
     for (int i = 0; i < ring.size(); i++) {
         a += ring[i].nodeAddress.getAddress() + " ";
     }
     const char * s = a.c_str();
    // old pre1 fails
    if (pre1.getHashCode() != haveReplicasOf[0].getHashCode() && pre2.getHashCode() == haveReplicasOf[1].getHashCode()) {
        s = ("pre 1 " + a).c_str();
        log->LOG(&memberNode->addr,s);
    }
    // old pre2 fails
    // promote secondary to primary locally
    else if (pre2.getHashCode() == haveReplicasOf[0].getHashCode() && pre2.getHashCode() != haveReplicasOf[1].getHashCode()) {
        s = ("pre 2 " + a).c_str();
        log->LOG(&memberNode->addr,s);
        // promte current secondary to primary
        // and send msg to post1 to promote tertiary to secondary
        // send msg to post2 to create tertiary
        for (auto it : ht->hashTable) {
            string value;
            int replicaType;
            getValueAndReplicaType(it.second,value,replicaType);
            
            if (static_cast<ReplicaType>(replicaType) == SECONDARY) {
                if (updateKeyValue(it.first,value,PRIMARY)) {
                    log->logUpdateSuccess(&memberNode->addr,false,0,it.first,value);
                }else {
                    log->logUpdateFail(&memberNode->addr,false,0,it.first,value);
                }
                
                // send to next two availiable nodes 
                Message msg_sec(0,memberNode->addr,UPDATE,it.first,value,SECONDARY);
                Message msg_ter(0,memberNode->addr,CREATE,it.first,value,TERTIARY);
                
                sendMsg(msg_sec,&(pre1.nodeAddress));
                sendMsg(msg_ter,&(pre2.nodeAddress));
            }
        }
    }
    
    // both old pre1 and old pre2 fail
    // promote both secondary and tertiary to primary locally
    else if (pre1.getHashCode() != haveReplicasOf[0].getHashCode() && pre2.getHashCode() != haveReplicasOf[1].getHashCode()) {
        s = ("pre 1 2 " + a).c_str();
        log->LOG(&memberNode->addr,s);
        for (auto it : ht->hashTable) {
            string value;
            int replicaType;
            getValueAndReplicaType(it.second,value,replicaType);
            
            if (static_cast<ReplicaType>(replicaType) == SECONDARY) {
                if (updateKeyValue(it.first,value,PRIMARY)) {
                    log->logUpdateSuccess(&memberNode->addr,false,0,it.first,value);
                }else {
                    log->logUpdateFail(&memberNode->addr,false,0,it.first,value);
                }
                //promoteTerToSec();
                // create ter
                // send to next two availiable nodes 
                Message msg_sec(0,memberNode->addr,UPDATE,it.first,value,SECONDARY);
                Message msg_ter(0,memberNode->addr,CREATE,it.first,value,TERTIARY);
                
                sendMsg(msg_sec,&(pre1.nodeAddress));
                sendMsg(msg_ter,&(pre2.nodeAddress));
            }
            if (static_cast<ReplicaType>(replicaType) == TERTIARY) {
                if (updateKeyValue(it.first,value,PRIMARY)) {
                    log->logUpdateSuccess(&memberNode->addr,false,0,it.first,value);
                }else {
                    log->logUpdateFail(&memberNode->addr,false,0,it.first,value);
                }
                
                // send to next two availiable nodes 
                Message msg_sec(0,memberNode->addr,CREATE,it.first,value,SECONDARY);
                Message msg_ter(0,memberNode->addr,CREATE,it.first,value,TERTIARY);
                
                sendMsg(msg_sec,&(pre1.nodeAddress));
                sendMsg(msg_ter,&(pre2.nodeAddress));
            }
        }
    }
    
    // only post2 fails
    if (post1.getHashCode() == hasMyReplicas[0].getHashCode() && post2.getHashCode() != hasMyReplicas[1].getHashCode()) {
        s = ("post 2 " + a).c_str();
        log->LOG(&memberNode->addr, s);
        for (auto it : ht->hashTable) {
            string value;
            int replicaType;
            getValueAndReplicaType(it.second,value,replicaType);
            
            if (static_cast<ReplicaType>(replicaType) == PRIMARY) {
                Message msg_ter(0,memberNode->addr,CREATE,it.first,value,TERTIARY);
                
                sendMsg(msg_ter,&(post2.nodeAddress));
            }    
        }
    }
    // only post1 fails
    else if (post1.getHashCode() != hasMyReplicas[0].getHashCode() && post1.getHashCode() == hasMyReplicas[1].getHashCode()) {
        s = ("post 1 " + a).c_str();
        log->LOG(&memberNode->addr,s);
        for (auto it : ht->hashTable) {
            string value;
            int replicaType;
            getValueAndReplicaType(it.second,value,replicaType);
            
            if (static_cast<ReplicaType>(replicaType) == PRIMARY) {
                Message msg_sec(0,memberNode->addr,UPDATE,it.first,value,SECONDARY);
                Message msg_ter(0,memberNode->addr,CREATE,it.first,value,TERTIARY);
                
                sendMsg(msg_sec,&(post1.nodeAddress));
                sendMsg(msg_ter,&(post2.nodeAddress));
            }    
        }
    }
    // both post1 and post2 fail
    else if (post1.getHashCode() != hasMyReplicas[0].getHashCode() && post2.getHashCode() != hasMyReplicas[1].getHashCode()) {
        s = ("post 1 2 " + a).c_str();
        log->LOG(&memberNode->addr,s);
        for (auto it : ht->hashTable) {
            string value;
            int replicaType;
            getValueAndReplicaType(it.second,value,replicaType);
            
            if (static_cast<ReplicaType>(replicaType) == PRIMARY) {
                Message msg_sec(0,memberNode->addr,CREATE,it.first,value,SECONDARY);
                Message msg_ter(0,memberNode->addr,CREATE,it.first,value,TERTIARY);
                
                sendMsg(msg_sec,&(post1.nodeAddress));
                sendMsg(msg_ter,&(post2.nodeAddress));
            }    
        }
    }
    
}

void MP2Node::getValueAndReplicaType(string str, string &value, int &replicaType) {
    int found = str.find(":");
    value = str.substr(0,found);
    str = str.substr(found + 1);
    
    found = str.find(":");
    replicaType = stoi(str.substr(found + 1));
}
