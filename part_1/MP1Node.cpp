/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;

    srand (time(NULL));
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        //log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        log->logNodeAdd(&memberNode->addr,&memberNode->addr);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }
    log->logNodeAdd(&memberNode->addr,&memberNode->addr);

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);

    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    Address *sender = (Address *)malloc(sizeof(Address));
    memcpy(sender->addr,data + 4,sizeof(Address));
    MsgTypes type = ((MessageHdr*)data)->msgType;
    
    if (type == JOINREQ) {
        // send back a JOINREP
        MessageHdr *msg;
        size_t msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(MemberListEntry) * (memberNode->memberList).size() + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        msg->msgType = JOINREP;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr),serializeMemberList(), sizeof(MemberListEntry) * (memberNode->memberList).size());
        emulNet->ENsend(&memberNode->addr, sender, (char *)msg, msgsize);

        // node exists 
        bool flag = false;
        for (int i = 0; i < memberNode->memberList.size(); i++) {
            if ((memberNode->memberList)[i].getid() == *(int *)(sender->addr)) {
                (memberNode->memberList)[i].settimestamp(par->getcurrtime());
                (memberNode->memberList)[i].setheartbeat(*(long *)(data + sizeof(MessageHdr) + 1 + sizeof(Address)));
                (memberNode->memberList)[i].setport(*(short *)(sender->addr + 4));

                flag = true;
                break;
            }
        }

        // add a new entry
        if (!flag) {
            MemberListEntry entry;
            entry.setid(*(int *)(sender->addr));
            entry.setport(*(short *)(sender->addr + 4));
            entry.setheartbeat(*(long *)(data + sizeof(MessageHdr) + 1 + sizeof(Address)));
            entry.settimestamp(par->getcurrtime());

            memberNode->memberList.push_back(entry);


            log->logNodeAdd(&memberNode->addr,sender);
        }
        
        free(sender);
        free(msg);

    }else if (type == JOINREP) {
        // init memberShipList coming from introducer
        memberNode->memberList = deserializeMemberList(data,size);
        memberNode->inGroup = true;

        for (int i = 0; i < memberNode->memberList.size(); i++) {
            Address *t = (Address*)malloc(sizeof(Address));
            int id = memberNode->memberList[i].getid();
            short port = memberNode->memberList[i].getport(); 
            memcpy(t,&id,sizeof(int));
            memcpy((char*)t + sizeof(int),&port,sizeof(short));
            
            log->logNodeAdd(&memberNode->addr,t);

            free(t);
        }

    }else if (type == GOSSIP) {
        // update my membership list
        vector<MemberListEntry> receivedMembershipList = deserializeMemberList(data,size);

        for (int i = 0; i < receivedMembershipList.size(); i++) {
            MemberListEntry newEntry = receivedMembershipList[i];
            if (newEntry.getid() == *(int *)(&memberNode->addr)) {
                continue;
            }
            
            bool exist = false;
            for (int j = 0; j < memberNode->memberList.size(); j++) {
                // next: update old member list
                MemberListEntry oldEntry = memberNode->memberList[j];
                if (newEntry.getid() == oldEntry.getid()) {
                    if (newEntry.getheartbeat() > oldEntry.getheartbeat()) {
                        memberNode->memberList[j] = newEntry;
                    }

                    exist = true;
                    break;
                }
            }

            if (!exist && par->getcurrtime() - newEntry.gettimestamp() < par->EN_GPSZ * 2) {
                memberNode->memberList.push_back(newEntry);

                Address *t = (Address*)malloc(sizeof(Address));
                int id = newEntry.getid();
                short port = newEntry.getport(); // pick the first element, for now
                memcpy(t,&id,sizeof(int));
                memcpy((char*)t + sizeof(int),&port,sizeof(short));

                log->logNodeAdd(&memberNode->addr,t);

                free(t);
            }
        }
    }
    
}

/*
 * deserilize membership array to vector
 */
vector<MemberListEntry> MP1Node::deserializeMemberList(char *data, int size) {
    int offset = sizeof(MessageHdr) + sizeof(Address) + 1;
    int memberSize = size - offset;
    vector<MemberListEntry> rt;

    while (memberSize > 0) {
        MemberListEntry *entry = (MemberListEntry *)(data + offset);
        rt.push_back(*entry);

        data += sizeof(MemberListEntry);
        memberSize -= sizeof(MemberListEntry);
    }    

    return rt;
}

/*
 * deserilize membership vector to array
 */
char* MP1Node::serializeMemberList() {
    size_t size = sizeof(MemberListEntry) * memberNode->memberList.size();
    MemberListEntry *rt = (MemberListEntry *)malloc(size * sizeof(char));

    for (int i = 0; i < memberNode->memberList.size(); i++) {
        memcpy( (char *)(rt + i), &memberNode->memberList[i], sizeof(MemberListEntry) );
    }

    return (char*)rt;
}

int MP1Node::getid() {
    return *(int *)memberNode->addr.addr;
}

void MP1Node::randomPickAndGossip() {
    Address *toaddr = (Address *)malloc(sizeof(Address));
    int ranID = rand() % memberNode->memberList.size();

    int id = (memberNode->memberList[ranID]).getid();
    short port = (memberNode->memberList[ranID]).getport(); // pick the first element, for now
    memcpy(toaddr,&id,sizeof(int));
    memcpy((char*)toaddr + sizeof(int),&port,sizeof(short));
    
    MessageHdr *msg;
    size_t msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(MemberListEntry) * memberNode->memberList.size() + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = GOSSIP;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr),serializeMemberList(), sizeof(MemberListEntry) * (memberNode->memberList).size());
    
    emulNet->ENsend(&memberNode->addr, toaddr, (char *)msg, msgsize);

    free(toaddr);
    free(msg);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    // remove dead node
    for (int i = 0; i < memberNode->memberList.size(); i++) {
        MemberListEntry entry = memberNode->memberList[i];
        if (entry.getid() == *(int *)(&memberNode->addr)) {
            memberNode->memberList.erase(memberNode->memberList.begin() + i);
            i--;
        }

        if ( par->EN_GPSZ * 2 + 10 < par->getcurrtime() - memberNode->memberList[i].gettimestamp()) {
          
            Address *toaddr = (Address *)malloc(sizeof(Address));
            int id = (memberNode->memberList[i]).getid();
            short port = (memberNode->memberList[i]).getport(); // pick the first element, for now
            memcpy(toaddr,&id,sizeof(int));
            memcpy((char*)toaddr + sizeof(int),&port,sizeof(short));

            log->logNodeRemove(&memberNode->addr,toaddr);
            
            free(toaddr);

            memberNode->memberList.erase(memberNode->memberList.begin() + i);
            i--;
        }
    }

    // increment heartbeat and add to list
    memberNode->heartbeat++;

    MemberListEntry entry;
    entry.setid(*(int *)(memberNode->addr.addr));
    entry.setport(*(short*)(memberNode->addr.addr + 4));
    entry.setheartbeat(memberNode->heartbeat);
    entry.settimestamp(par->getcurrtime());
    memberNode->memberList.push_back(entry);
    // pick a neighbor randomly and gossip
    randomPickAndGossip();

    return;
}



/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
