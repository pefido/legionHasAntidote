var PORT = 8004;

var PeerSync = require('./../shared/peerSync.js').PeerSync;
var CRDT_Database = require('./CRDT_Database.js').CRDT_Database;
var ServerMessaging = require('./ServerMessaging.js').ServerMessaging;

var Compressor = require('./../shared/Compressor.js');
var D = require('./../shared/Duplicates.js');
var ALQueue = require('./../shared/ALQueue.js').ALQueue;
var ALMap = require('./../shared/ALMap.js').ALMap;

var CRDT = require('./../shared/crdt.js').CRDT;

var antidoteClient = require('./antidoteClient.js');

var CRDT_LIB = {};
CRDT_LIB.STATE_Counter = require('./../shared/crdtLib/stateCounter.js').STATE_Counter;
CRDT_LIB.OP_ORSet = require('./../shared/crdtLib/opSet.js').OP_ORSet;
CRDT_LIB.OP_ORMap = require('./../shared/crdtLib/opMap.js').OP_ORMap;
CRDT_LIB.STATE_Set = require('./../shared/crdtLib/stateSet.js').STATE_Set;
CRDT_LIB.DELTA_Set = require('./../shared/crdtLib/deltaSet.js').DELTA_Set;

var util = require('util');
var WebSocket = require('ws');
var storage = require('node-persist');

var WebSocketServer;
var wss;

initService();
function initService() {
    WebSocketServer = WebSocket.Server;
    wss = new WebSocketServer({
        port: PORT, verifyClient: function (info, cb) {
            cb(true);
        }
    });

    var peerSyncs = new ALMap();
    var messagingAPI = new ServerMessaging(peerSyncs);

    /**
     * CRDT Database - contains all known crdts.
     * @type {CRDT_Database}
     */
    var db = new CRDT_Database(messagingAPI, peerSyncs);

    db.defineCRDT(CRDT_LIB.STATE_Counter);
    db.defineCRDT(CRDT_LIB.OP_ORSet);
    db.defineCRDT(CRDT_LIB.OP_ORMap);
    db.defineCRDT(CRDT_LIB.STATE_Set);
    db.defineCRDT(CRDT_LIB.DELTA_Set);

    { // Client connection handling.
        var duplicates = new D.Duplicates();
        var nodes = [];

        wss.on('connection', function (socket) {
                var os = {
                    id: "localhost:8004",
                    messageCount: 0
                };

                util.log("Connection.");
                nodes.push(socket);
                db.id = os.id;

                /**
                 * For generating messages that can be sent.
                 * Type is required.
                 * Data (optional) is compressed to save bandwidth.
                 * @param type {String}
                 * @param data {Object}
                 * @param callback {Function}
                 */

                db.versionVectorDiff = CRDT.versionVectorDiff;


                var ps = new PeerSync(db, db, socket);
                db.handlers = {
                    peerSync: {
                        //The order is, when clients syncs objects are initiated on the server side.
                        //Only then can the server sync as this is when he HAS them.
                        //Only on PSA will the objects have the client's changes.
                        type: "OS:PS", callback: function (message) {
                            //util.log("AA1" + JSON.stringify(message));
                            antidoteClient.treatMessage(message, db);
                            ps.handleSync(message);
                            ps.sync();
                        }
                    },
                    peerSyncAnswer: {
                        type: "OS:PSA", callback: function (message) {
                            //util.log("AA2" + JSON.stringify(message));
                            ps.handleSyncAnswer(message, db);
                        }
                    },
                    gotContentFromNetwork: {
                        type: "OS:C", callback: function (message, original, connection) {
                            //util.log("AA3" + JSON.stringify(message));
                            antidoteClient.treatMessage(message, db);
                            db.gotContentFromNetwork(message, original, connection);
                        }
                    },
                    version_vector_propagation: {
                        type: "OS:VVP", callback: function (message, original, connection) {
                            //util.log("AA4" + JSON.stringify(message));
                            antidoteClient.treatMessage(message, db);
                            db.gotVVFromNetwork(message, original, connection);
                        }
                    }
                };

                socket.on('message', function incoming(message) {
                    var parsed = JSON.parse(message);
                    console.log("Got " + parsed.type + " from " + socket.remoteID + " s: " + parsed.sender);


                    if (!duplicates.contains(parsed.sender, parsed.ID)) {
                        duplicates.add(parsed.sender, parsed.ID);
                        var original = JSON.parse(message);

                        var cb = function (parsed) {
                            if (parsed.type == "CLIENT_ID") {
                                console.log("New client: " + parsed.clientID);
                                socket.remoteID = parsed.clientID;
                                peerSyncs.set(socket.remoteID, ps);
                                return;
                            }

                            if (parsed.type == db.handlers.peerSync.type) {
                                console.log("peerSync");
                                db.handlers.peerSync.callback(parsed, original, socket);
                            } else if (parsed.type == db.handlers.peerSyncAnswer.type) {
                                console.log("peerSyncAnswer");
                                db.handlers.peerSyncAnswer.callback(parsed, original, socket);
                            } else if (parsed.type == db.handlers.gotContentFromNetwork.type) {
                                console.log("contentFromNetwork");
                                db.handlers.gotContentFromNetwork.callback(parsed, original, socket);
                            } else if (parsed.type == db.handlers.version_vector_propagation.type) {
                                console.log("versionVectorProp");
                                db.handlers.version_vector_propagation.callback(parsed, original, socket);
                            } else {
                                util.error("Unkown message type.");
                                util.log(JSON.stringify(parsed));
                            }
                        };

                        if (parsed.content) {
                            Compressor.decompress(parsed.content, function (result) {
                                parsed.content = JSON.parse(result);
                                cb(parsed);
                            });
                        } else {
                            Compressor.decompress("5d00000100040000000000000000331849b7e4c02e1ffffac8a000", function (result) {
                                cb(parsed);
                            });
                        }
                    } else {
                        util.log("Duplicate.")
                    }
                });
                socket.on('disconnect', function () {
                    util.log("Disconnected.");
                });
            }
        )
    }
}
