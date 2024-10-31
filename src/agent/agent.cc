// SPDX-License-Identifier: Apache-2.0

#include <pthread.h>

#include <boost/timer/timer.hpp>
#include <glog/logging.h>
#include <zmq.hpp>

#include "agent.hh"
#include "../common/config.hh"
#include "../common/io.hh"
#include "../common/coding/coding_util.hh"
#include "../common/util.hh"

Agent::Agent() {
    _cxt = zmq::context_t(1);
    _io = new AgentIO(&_cxt);
    _numWorkers = Config::getInstance().getAgentNumWorkers();
    _containerManager = new ContainerManager();
    _coordinator = new AgentCoordinator(_containerManager);
    pthread_mutex_init(&_stats.lock, NULL);

    // init statistics
    _stats.traffic = {0, 0};
    _stats.chunk = {0, 0};
    _stats.ops = {0, 0};
}

Agent::~Agent() {
    LOG(WARNING) << "Terminating Agent ...";

    // stop the proxy for delivering chunk events (so the workers will stop)
    delete _io;
    _cxt.close();

    // join worker threads
    for (int i = 0; i < _numWorkers; i++)
        pthread_join(_workers[i], NULL);

    // wait the workers to end working with the coordinator and container manager
    delete _coordinator;
    delete _containerManager;

    LOG(WARNING) << "Terminated Agent";
}

void Agent::run(bool reg) {
    // register itself to proxies
    if (reg && _coordinator->registerToProxy() == false) {
        LOG(ERROR) << "Failed to register to Proxy";
        return;
    }

    // run chunk event handling workers
    for (int i = 0; i < _numWorkers; i++)
        pthread_create(&_workers[i], NULL, handleChunkEvent, (void *) this);

    // listen to incoming requests
    _io->run(_workerAddr);
}

void *Agent::handleChunkEvent(void *arg) {
    // get benchmark instance
    // Benchmark &bm = Benchmark::getInstance();

    Agent *self = (Agent*) arg;
    
    // connect to the worker proxy socket
    zmq::socket_t socket(self->_cxt, ZMQ_REP);
    Util::setSocketOptions(&socket, AGENT_TO_PROXY);
    try {
        socket.connect(self->_workerAddr);
    } catch (zmq::error_t &e) {
        LOG(ERROR) << "Failed to connect to event queue: " << e.what();
        return NULL;
    }

    // start processing events distributed by the worker proxy
    while(true) {
        ChunkEvent event;
        unsigned long int traffic = 0;

        boost::timer::cpu_timer mytimer;

        // TAGPT(start): agent listening to chunk event message
        TagPt tagPt_getCnkEvMsg;
        TagPt tagPt_agentProcess;
        TagPt tagPt_rep2Pxy;

        tagPt_getCnkEvMsg.markStart();

        // get next event
        try {
            // get message and translate the message back into an event
            traffic = IO::getChunkEventMessage(socket, event);
            self->addIngressTraffic(traffic);
        } catch (zmq::error_t &e) {
            LOG(ERROR) << "Failed to get chunk event message: " << e.what();
            break;
        }

        // TAGPT(end): agent listening to chunk event message
        tagPt_getCnkEvMsg.markEnd();

        //DLOG(INFO) << "Get a chunk event message in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
        mytimer.start();

        traffic = 0;
        // process the event
        switch(event.opcode) {
        case Opcode::PUT_CHUNK_REQ:
            // TAGPT(start): agent put chunk
            tagPt_agentProcess.markStart();

            if (event.containerIds == NULL) {
                LOG(ERROR) << "[PUT_CHUNK_REQ] Failed to allocate memory for container ids";
                exit(1);
            }

            // Now event.chunks[i].p2a (startTv, endTv) are marked with valid time

            if (self->_containerManager->putChunks(event.containerIds, event.chunks, event.numChunks) == true) {            
                event.opcode = Opcode::PUT_CHUNK_REP_SUCCESS;
                self->incrementOp();

                // TAGPT(end): agent put chunk
                tagPt_agentProcess.markEnd();

                // LOG(INFO) << std::fixed << std::setprecision(6)
                // << tagPt_agentProcess.startTv.sec() << ", " << tagPt_agentProcess.endTv.sec();

                 LOG(INFO) << "Put " << event.numChunks << " chunks into containers speed = " 
                           << event.numChunks * event.chunks[0].size * 1.0 / (1 << 20) / (mytimer.elapsed().wall * 1.0 / 1e9) 
                           << "MB/s , in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
            } else {
                event.opcode = Opcode::PUT_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to put " << event.numChunks << " chunks into containers";
                self->incrementOp(false);
            }
            for (int i = 0; i < event.numChunks; i++) {
                traffic += event.chunks[i].size;
            }
            self->addIngressChunkTraffic(traffic);

            break;

        case Opcode::GET_CHUNK_REQ:
            // TAGPT(start): agent get required chunks from container
            tagPt_agentProcess.markStart();

            if (self->_containerManager->getChunks(event.containerIds, event.chunks, event.numChunks) == true) {
                // TAGPT(end): agent listening to chunk event message
                tagPt_agentProcess.markEnd();
                int totalChunkSize = 0;

                for (int i = 0; i < event.numChunks; i++) {
                    totalChunkSize += event.chunks[i].size;
                }

                event.opcode = Opcode::GET_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Get " << event.numChunks << " chunks from containers speed = " 
                          << event.numChunks * event.chunks[0].size * 1.0 / (1 << 20) / (mytimer.elapsed().wall * 1.0 / 1e9) 
                          << "MB/s , in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";

                for (int i = 0; i < event.numChunks; i++) {
                    traffic += event.chunks[i].size;
                }

                self->addEgressChunkTraffic(traffic);
                self->incrementOp();
            } else {
                event.opcode = Opcode::GET_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to get " << event.numChunks << " chunks from containers";
                self->incrementOp(false);
            }
            break;

        case Opcode::DEL_CHUNK_REQ:
            // TAGPT(start): agent del chunk
            tagPt_agentProcess.markStart();

            if (self->_containerManager->deleteChunks(event.containerIds, event.chunks, event.numChunks) == true) {
                event.opcode = Opcode::DEL_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Delete " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();

                // TAGPT(end): agent del chunk
                tagPt_agentProcess.markEnd();

            } else {
                event.opcode = Opcode::DEL_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to delete " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            break;

        case Opcode::CPY_CHUNK_REQ:
            if (self->_containerManager->copyChunks(event.containerIds, event.chunks, &(event.chunks[event.numChunks]), event.numChunks) == true) {
                event.opcode = Opcode::CPY_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Copy " << event.numChunks << " chunks in containers speed = " 
                          << event.numChunks * event.chunks[0].size / (mytimer.elapsed().wall * 1.0 / 1e9) 
                          << "MB/s , in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();
            } else {
                event.opcode = Opcode::CPY_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to copy " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            // move the chunk metadata forward for reply
            for (int i = 0; i < event.numChunks; i++)
                event.chunks[i].copyMeta(event.chunks[event.numChunks]);
            break;

        case Opcode::ENC_CHUNK_REQ:
            {
                Chunk *encodedChunk = new Chunk[1];
                if (encodedChunk == NULL) {
                    LOG(ERROR) << "Failed to allocate memory for encoded chunk";
                } else {
                    *encodedChunk = self->_containerManager->getEncodedChunks(event.containerIds, event.chunks, event.numChunks, event.codingMeta.codingState);
                }
                if (encodedChunk && encodedChunk->size > 0) {
                    ChunkEvent temp = event; // let the original event be freed
                    LOG(INFO) << "Encode " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                    event.opcode = Opcode::ENC_CHUNK_REP_SUCCESS;
                    event.numChunks = 1;
                    event.chunks = encodedChunk;     // free the chunk pointer after the event is sent
                    event.chunks[0].freeData = true; // free after the event is sent
                    event.containerIds = 0;
                    event.codingMeta = CodingMeta();
                    self->incrementOp();
                } else {
                    event.opcode = Opcode::ENC_CHUNK_REP_FAIL;
                    LOG(ERROR) << "Failed to encode " << event.numChunks << " chunks in containers";
                    self->incrementOp(false);
                }
                break;
            }

        case Opcode::RPR_CHUNK_REQ:
            // check if coding scheme is valid
            if (event.codingMeta.coding < 0 || event.codingMeta.coding >= CodingScheme::UNKNOWN_CODE) { 
                LOG(ERROR) << "Invalid coding scheme " << (int) event.codingMeta.coding;
                event.opcode = Opcode::RPR_CHUNK_REP_FAIL;
                break;
            } 
        { // scope for declaring variables..
            // start repairing
            bool isCAR = event.repairUsingCAR;
            bool useEncode = isCAR;
            int numChunksPerNode = 1;
            int numInputChunkReq = isCAR? event.numChunkGroups : event.chunkGroupMap[0];
            int numInputChunkReqSent = 0;
            // construct the requests for input chunks
            ChunkEvent getInputEvents[numInputChunkReq * 2];
            IO::RequestMeta meta[numInputChunkReq];
            pthread_t rt[numInputChunkReq];
            unsigned char matrix[numInputChunkReq];
            unsigned char namespaceId = event.chunks[0].getNamespaceId();
            boost::uuids::uuid fileuuid = event.chunks[0].getFileUUID();
            int version = event.chunks[0].getFileVersion();
            //DLOG(INFO) << "Number of chunk groups = " << event.numChunkGroups << " address " << event.agents;
            
            DLOG(INFO) << "START of chunk repair useCar = " << isCAR << " numInputChunkReq = " << numInputChunkReq;
            int chunkPos = 0; // chunk list starting position
            int agentAddrStPos = 0, agentAddrEdPos = 0; // agent address positions
            for (int reqIdx = 0; reqIdx < numInputChunkReq; reqIdx++, numInputChunkReqSent++) {
                // set up the get-input-chunk event
                ChunkEvent &curEvent = getInputEvents[reqIdx];
                int numChunks = isCAR? event.chunkGroupMap[reqIdx + chunkPos] : numChunksPerNode;
                curEvent.id = self->_eventCount.fetch_add(1);
                // ask the agent to partial encode the input chunks when using CAR, otherwise get the original chunk
                curEvent.opcode = useEncode? Opcode::ENC_CHUNK_REQ : Opcode::GET_CHUNK_REQ;
                curEvent.numChunks = numChunks;
                curEvent.containerIds = &event.containerGroupMap[chunkPos];
                try {
                    curEvent.chunks = new Chunk[numChunks];
                } catch (std::bad_alloc &e) {
                    // abort if the agent runs out of memory
                    LOG(ERROR) << "Failed to allocate memory for " << numChunks << " chunks";
                    break;
                }
                for (int chunkIdx = 0; chunkIdx < numChunks; chunkIdx++) {
                    int cid = isCAR? event.chunkGroupMap[chunkPos + reqIdx + chunkIdx + 1]:
                            event.chunkGroupMap[chunkPos + chunkIdx + 1];
                    Chunk &curChunk = curEvent.chunks[chunkIdx];
                    curChunk.setId(namespaceId, fileuuid, cid);
                    curChunk.size = 0;
                    curChunk.data = NULL;
                    curChunk.fileVersion = version;
                }
                if (isCAR) {
                    curEvent.codingMeta.codingStateSize = numChunks;
                    curEvent.codingMeta.codingState = &event.codingMeta.codingState[chunkPos];
                    // set the final decoding matrix coefficient to 1 for XOR operations on the partial encoded chunks
                    matrix[reqIdx] = 1;
                } else if (useEncode) {
                    curEvent.codingMeta.codingStateSize = numChunks;
                    curEvent.codingMeta.codingState = matrix + reqIdx;
                    // set the final decoding matrix coefficient to 1 for XOR operations on the partial encoded chunks
                    matrix[reqIdx] = 1;
                }
                IO::RequestMeta &curMeta = meta[reqIdx];
                // set up the request-response metadata pair
                curMeta.isFromProxy = false;
                curMeta.containerId = event.containerGroupMap[chunkPos];
                curMeta.cxt = &(self->_cxt);
                agentAddrEdPos = event.agents.find(';', agentAddrStPos);
                curMeta.address = event.agents.substr(agentAddrStPos, agentAddrEdPos - agentAddrStPos);
                agentAddrStPos = agentAddrEdPos + 1;
                curMeta.request = &getInputEvents[reqIdx];
                curMeta.reply = &getInputEvents[numInputChunkReq + reqIdx];
                // send the request
                pthread_create(&rt[reqIdx], NULL, IO::sendChunkRequestToAgent, (void *) &meta[reqIdx]);
                // increment chunk list position
                chunkPos += numChunks;
            }
            // check the chunk replies
            bool allsuccess = numInputChunkReq == numInputChunkReqSent;
            unsigned char *input[numInputChunkReq], *output[event.numChunks];
            int chunkSize = 0;
            for (int reqIdx = 0; reqIdx < numInputChunkReqSent; reqIdx++) {
                // wait for the request to complete
                void *ptr = NULL;
                pthread_join(rt[reqIdx], &ptr);
                IO::RequestMeta &curMeta = meta[reqIdx];
                ChunkEvent &curReqEvent = getInputEvents[reqIdx];
                // avoid freeing reference to local variables
                curReqEvent.containerIds = nullptr; 
                curReqEvent.codingMeta.codingState = NULL; 
                Opcode expectedOp = useEncode? ENC_CHUNK_REP_SUCCESS : GET_CHUNK_REP_SUCCESS;
                if (ptr != NULL || curMeta.reply->opcode != expectedOp) {
                    LOG(ERROR) << "Failed to operate on chunk due to internal failure, container id = " << curMeta.containerId << ", return opcode =" << curMeta.reply->opcode;
                    allsuccess = false;
                    continue;
                }
                input[reqIdx] = curMeta.reply->chunks[0].data;
                chunkSize = curMeta.reply->chunks[0].size;
            }
            // start repair after getting all required chunks
            if (allsuccess) {
                for (int chunkIdx = 0; chunkIdx < event.numChunks; chunkIdx++) {
                    Chunk &curChunk = event.chunks[chunkIdx];
                    curChunk.data = (unsigned char *) malloc (chunkSize);
                    if (curChunk.data == NULL) {
                        LOG(ERROR) << "Failed to allocate memeory for storing repaired chunks (" << chunkIdx << " of " << event.numChunks - 1 << "chunks)";
                        allsuccess = false;
                        break;
                    }
                    curChunk.size = chunkSize;
                    output[chunkIdx] = curChunk.data;
                }
                // do decoding
                CodingUtils::encode(input, numInputChunkReq, output, event.numChunks, chunkSize, isCAR? matrix : event.codingMeta.codingState);
                // compute checksum
                for (int chunkIdx = 0; chunkIdx < event.numChunks; chunkIdx++) {
                    event.chunks[chunkIdx].computeMD5();
                }
                // send chunks to other agents for storage (keep first numChunksPerNode for local storage, and send out the remaining)
                int numChunksToSend = isCAR? 0 : event.numChunks - numChunksPerNode;
                int numChunkReqsToSend = numChunksToSend / numChunksPerNode;
                int numChunkReqsSent = 0;
                ChunkEvent storeChunkEvents[numChunkReqsToSend * 2];
                IO::RequestMeta storeChunkMeta[numChunkReqsToSend];
                pthread_t wt[numChunkReqsToSend];
                for (int reqIdx = 0; reqIdx < numChunkReqsToSend; reqIdx++, numChunkReqsSent++) {
                    ChunkEvent &curEvent = storeChunkEvents[reqIdx];
                    // setup the request
                    curEvent.id = self->_eventCount.fetch_add(1);
                    curEvent.opcode = Opcode::PUT_CHUNK_REQ;
                    curEvent.numChunks = numChunksPerNode;
                    try {
                        curEvent.chunks = new Chunk[curEvent.numChunks];
                        curEvent.containerIds = new int[curEvent.numChunks];
                    } catch (std::bad_alloc &e) {
                        LOG(ERROR) << "Failed to allocate memeory for sending repaired chunks (" << reqIdx << " of " << event.numChunks - 1 << "chunks)";
                        delete curEvent.chunks;
                        delete curEvent.containerIds;
                        allsuccess = false;
                        break;
                    }
                    // mark the chunks
                    for (int chunkIdx = 0; chunkIdx < curEvent.numChunks; chunkIdx++) {
                        Chunk &curChunk = curEvent.chunks[chunkIdx];
                        curChunk = event.chunks[(reqIdx + 1) * curEvent.numChunks + chunkIdx];
                        curChunk.freeData = false;
                        curEvent.containerIds[chunkIdx] = event.containerIds[reqIdx + 1];  // skip the first container id for local chunk storage
                    }
                    // setup the io meta for request and reply
                    IO::RequestMeta &curMeta = storeChunkMeta[reqIdx];
                    curMeta.containerId = event.containerIds[reqIdx + 1]; // skip the first container id for local chunk storage
                    curMeta.request = &storeChunkEvents[reqIdx];
                    curMeta.reply = &storeChunkEvents[reqIdx + numChunkReqsToSend];
                    curMeta.isFromProxy = false;
                    curMeta.cxt = &(self->_cxt);
                    agentAddrEdPos = event.agents.find(';', agentAddrStPos);
                    curMeta.address = event.agents.substr(agentAddrStPos, agentAddrEdPos - agentAddrStPos);
                    agentAddrStPos = agentAddrEdPos + 1;
                    // send the request and wait for reply
                    pthread_create(&wt[reqIdx], NULL, IO::sendChunkRequestToAgent, (void *) &storeChunkMeta[reqIdx]);
                }
                if (numChunkReqsSent == numChunkReqsToSend) {
                    int numLocalChunks = isCAR? event.numChunks : numChunksPerNode;
                    int localContainerIds[numLocalChunks];
                    for (int i = 0; i < numLocalChunks; i++)
                        localContainerIds[i] = event.containerIds[0];
                    // put chunk locally
                    if (self->_containerManager->putChunks(localContainerIds, event.chunks, numLocalChunks) == true) {
                        LOG(INFO) << "Put " << numLocalChunks << " repaired chunks into containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                    } else {
                        LOG(ERROR) << "Failed to put " << numLocalChunks << " repaired chunks into containers";
                        allsuccess = false;
                    }
                }
                for (int reqIdx = 0; reqIdx < numChunkReqsSent; reqIdx++) {
                    void *ptr = 0;
                    // check if other chunks are stored successfully
                    pthread_join(wt[reqIdx], &ptr);
                    IO::RequestMeta &curMeta = storeChunkMeta[reqIdx];
                    if (ptr != 0 || curMeta.reply->opcode != Opcode::PUT_CHUNK_REP_SUCCESS) {
                        LOG(ERROR) << "Failed to put " << curMeta.request->numChunks 
                                   << " repaired chunk (" << curMeta.request->chunks[0].getChunkId() << ")"
                                   << " to container " << curMeta.containerId
                                   << " at " << curMeta.address;
                        allsuccess = false;
                    }
                    // 'best-effort revert' by deleting successfully stored chunks due to the failure of others
                    // TODO support partial success of a repair request at the proxy
                    if (numChunksToSend != numChunkReqsSent) {
                        IO::RequestMeta &curMeta = storeChunkMeta[reqIdx];
                        curMeta.request->opcode = Opcode::DEL_CHUNK_REQ;
                        IO::sendChunkRequestToAgent(&curMeta);
                    }
                }
            }
            DLOG(INFO) << "END of chunk repair useCar = " << isCAR << " numInputChunkReq = " << numInputChunkReq;
            // set reply, increment op count
            if (allsuccess) {
                event.opcode = Opcode::RPR_CHUNK_REP_SUCCESS;
                self->incrementOp();
            } else {
                event.opcode = RPR_CHUNK_REP_FAIL;
                self->incrementOp(false);
            }
        } // scope for declaring variables..
            break;

        case CHK_CHUNK_REQ:
            if (self->_containerManager->hasChunks(event.containerIds, event.chunks, event.numChunks)) {
                LOG(INFO) << "Checked " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                event.opcode =  Opcode::CHK_CHUNK_REP_SUCCESS;
            } else {
                LOG(ERROR) << "Failed to find (some of) " << event.numChunks << " chunks in containers for checking";
                event.opcode = Opcode::CHK_CHUNK_REP_FAIL;
            }
            break;

        case MOV_CHUNK_REQ:
            if (self->_containerManager->moveChunks(event.containerIds, event.chunks, &(event.chunks[event.numChunks]), event.numChunks) == true) {
                event.opcode = Opcode::MOV_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Move " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();
            } else {
                event.opcode = Opcode::MOV_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to move " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            // move the chunk metadata forward for reply
            for (int i = 0; i < event.numChunks; i++)
                event.chunks[i].copyMeta(event.chunks[event.numChunks]);
            break;

        case RVT_CHUNK_REQ:
            if (self->_containerManager->revertChunks(event.containerIds, event.chunks, event.numChunks) == true) {
                event.opcode = Opcode::RVT_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Revert " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();
            } else {
                event.opcode = Opcode::RVT_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to revert " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            break;

        case VRF_CHUNK_REQ:
            { 
                int numCorruptedChunks = 0;
                if ((numCorruptedChunks = self->_containerManager->verifyChunks(event.containerIds, event.chunks, event.numChunks)) >= 0) {
                    // report only corrupted chunks (in-place replaced by the function call)
                    LOG(INFO) << "Verify checksums " << event.numChunks << " chunks (" << numCorruptedChunks << " failed) in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                    event.numChunks = numCorruptedChunks;
                    event.opcode = Opcode::VRF_CHUNK_REP_SUCCESS;
                    self->incrementOp();
                } else {
                    event.opcode = Opcode::VRF_CHUNK_REP_FAIL;
                    LOG(ERROR) << "Failed to verify checksums for " << event.numChunks << " chunks in containers";
                    self->incrementOp(false);
                }
            }
        }

        // send a reply
        try {
            
            // TAGPT(start): agent send reply to proxy
            tagPt_rep2Pxy.markStart();

            // copy from tagpts to event
            event.p2a.getEnd() = tagPt_getCnkEvMsg.getEnd();
            event.agentProcess = tagPt_agentProcess;
            event.a2p.getStart() = tagPt_rep2Pxy.getStart();

            // mytimer.start();

            traffic = IO::sendChunkEventMessage(socket, event);

            self->addEgressTraffic(traffic);

            // TAGPT(end): agent send reply to proxy
            tagPt_rep2Pxy.markEnd();

            // DLOG(INFO) << "Send reply in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";

        } catch (zmq::error_t &e) {
            LOG(ERROR) << "Failed to send chunk event message: " << e.what();
            break;
        }
    }

    return NULL;
}

void Agent::addIngressTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.traffic.in += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::addEgressTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.traffic.out += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::addEgressChunkTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.chunk.out += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::addIngressChunkTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.chunk.in += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::incrementOp(bool success) {
    pthread_mutex_lock(&_stats.lock);
    if (success)
        _stats.ops.success++;
    else
        _stats.ops.fail++;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::printStats() {
    printf(
        "----- Agent Stats -----\n"
        "Total Traffic   (in) %10lu (out)  %10lu\n"
        "Chunk Traffic   (in) %10lu (out)  %10lu\n"
        "Operation count (ok) %10lu (fail) %10lu\n"
        "-----------------------\n"
        , _stats.traffic.in
        , _stats.traffic.out
        , _stats.chunk.in
        , _stats.chunk.out
        , _stats.ops.success
        , _stats.ops.fail
    );
}
