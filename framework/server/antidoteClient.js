if (typeof exports != "undefined") {
  exports.fetchCounterProto = fetchCounterProto;
  exports.incrementCounterProto = incrementCounterProto;
  exports.treatMessage = treatMessage;
}

const http = require('http');
zookeeper = require('node-zookeeper-client');
var dcid = [];
var legionDb;
var firstTime = true;
//var sentOps = {};
var tokensLegionToAntidote = {};
var storedObjects = {};
var lastSeenTimestamp = 0;
var lastCommitTimestamp = 0;

var zooClient = zookeeper.createClient('localhost:2181');

zooClient.once('connected', function () {
  console.log('Connected to the zoo.');

  /*client.create(path, function (error) {
    if (error) {
      console.log('Failed to create node: %s due to: %s.', path, error);
    } else {
      console.log('Node: %s is successfully created.', path);
    }

    client.close();
  });*/
});

zooClient.connect();

/**
 * ping antidote
 */
function ping() {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };
  xhttp.open( "GET", 'http://localhost:8088/ping', true);
  xhttp.send();
}

function pingProto() {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };
  xhttp.open( "GET", 'http://localhost:8088/pingProto', true);
  xhttp.send();
}

/******************Counter Operations******************/

/**
 * create counter
 * @param bType - riak bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 */
function createCounter(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key
  });

  xhttp.open( "POST", 'http://localhost:8088/createCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/**
 * delete counter
 * @param bType - riak bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 */
function deleteCounter(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key
  });

  xhttp.open( "PUT", 'http://localhost:8088/deleteCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/**
 * fetch counter
 * @param bType - antidote bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 */
function fetchCounter(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchCounter'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function fetchCounterProto (key){
  var http = require('http');
  var data = '/' + key;
  return http.get({
    host: 'localhost',
    port: 8088,
    path: '/fetchCounterProto' + data
  }, function(response) {
    let body = '';
    response.on('data', function(chunk) {
      body += chunk;
    });
    response.on('end', function() {
      //console.log(body);
    });
  });
}

/**
 * increment/decrement counter
 * @param bType - riak bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 * @param increment - the value to increment or negative if decrement
 */
function incrementCounter(bType, bucket, key, increment){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key,
    increment: increment
  });

  xhttp.open( "PUT", 'http://localhost:8088/incrementCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function incCounter(){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({});

  xhttp.open( "PUT", 'http://localhost:8088/incCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function decCounter(){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({});

  xhttp.open( "PUT", 'http://localhost:8088/decCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/**
 * increment/decrement counter
 * @param bType - doesnt matter
 * @param bucket - doesnt matter
 * @param key - antidote key
 * @param increment - always increments 1
 */
function incrementCounterProto (key, increment){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    key: key,
    increment: increment
  });

  xhttp.open( "PUT", 'http://localhost:8088/incrementCounterProto', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function decrementCounterProto(bType, bucket, key, increment){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key,
    increment: increment
  });

  xhttp.open( "PUT", 'http://localhost:8088/decrementCounterProto', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function fetchObject(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchObject'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function fetchObjectProto(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchObjectProto'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function createObject(bType, bucket, key, value){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key,
    value: value
  });

  xhttp.open( "POST", 'http://localhost:8088/createObject', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/******************Set Operations******************/

function fetchSet(bType, bucket, key) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchSet'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function fetchSetProto(key) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchSetProto'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function createSet(bType, bucket, key) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key
  });

  xhttp.open( "POST", 'http://localhost:8088/createSet', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function addToSet(bType, bucket, key, additions) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key,
    additions: additions
  });

  xhttp.open( "PUT", 'http://localhost:8088/addToSet', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/******************Send operations from legion to Antidote******************/

function treatMessage(message, db) {
  if(firstTime) {
    legionDb = db;
    firstTime = false;
  }

  switch(message.type) {
    case 'OS:C':
      let crdtType = db.getCRDT(message.content.objectID).crdt.type;
      console.log(JSON.stringify(message));
      //console.log(message);
      switch(crdtType) {
        case 'STATE_Counter':
          //update state
          break;

        case 'OP_ORSet':
          //console.log(message.content.operations);
          message.content.operations.forEach(function(element) {
            switch(element.opName) {
              case 'add':
                getObjects(message.content.objectID, 'crdt_orset', ['checkAndUpdate', 'add', element.result.element, element.result.unique, [dcid[0], dcid[1], dcid[2], dcid[3], lastCommitTimestamp]]);
                break;
              case 'remove':
                getObjects(message.content.objectID, 'crdt_orset', ['checkAndUpdate', 'remove', element.result.element, element.result.removes[0], [dcid[0], dcid[1], dcid[2], dcid[3], lastCommitTimestamp]]);
                break;
            }
          });
          break;

        case 'OP_ORMap':
          //update op
          break;
      }
      break;

    case 'OS:PS':
      //peerSync
      break;

    case 'OS:VVP':
      console.log("case VVP");
      let crdt = db.getCRDT(message.content.objectID).crdt.crdt;
      console.log(message);
      break;
  }
}


/******************Object Operations******************/

function getObjects(key, type, next, lockNode) {
  //console.log('lockNode at getObjects: ' + lockNode);

  if(next[0] == 'checkAndUpdate' || (next[0] == 'sync' && key != 'sentOps'))
    var data = '/' + key + '_tokens' + '/' + type;
  else var data = '/' + key + '/' + type;

  zooExistsAndCreate('/' + key, lockNode, function(lockNode) {

      zooCreateEfem('/' + key + '/' + key, lockNode, function(lockNode) {
        http.get({
          host: 'localhost',
          port: 8088,
          path: '/getObjects' + data
        }, function(response) {
          let body = '';
          response.on('data', function(chunk) {
            body += chunk;
          });
          response.on('end', function() {
            //console.log(body);
            let jsonBody = JSON.parse(body);

            switch (next[0]) {
              case 'firstTime':
                unlockNode(lockNode);
                dcid = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[0].dcid;
                console.log(dcid);
                lastSeenTimestamp = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[1];
                lastCommitTimestamp = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[1];
                break;
              case 'updateTime':
                lastSeenTimestamp = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[1];
                lastCommitTimestamp = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[1];
                unlockNode(lockNode);
                break;
              case 'checkAndUpdate':
                checkUpdate(key, type);
                //ver se o token já lá está
                let tokens = jsonBody.success.get_objects_resp[0].object_and_clock[0].orset;
                let found = false;
                tokens.forEach(function(element, index, array) {
                  let token = element.element[0].json_value.split(" ");
                  if(token[0] == next[3])
                    found = true;
                });
                if(next[1] == 'add') {
                  if(!found)
                    updateObjectsSnapshot(key, type, next[1], next[2], next[3], next[4], lockNode);
                  else console.log('duplicated add operation');
                }
                else if(next[1] == 'remove') {
                  if(found)
                    updateObjectsSnapshot(key, type, next[1], next[2], next[3], next[4], lockNode);
                  else console.log('duplicated remove operation');
                }
                break;
              case 'sync':
                if(key == 'sentOps') {
                  //console.log('lockNode at sentOps: ' + lockNode);
                  let next2 = next;
                  next2[4] = jsonBody.success.get_objects_resp[0].object_and_clock[0].orset;
                  getLogOps(next2[2], type, next2[1], [next2[0], next2[3], next2[4]], lockNode);
                }
                else {
                  //console.log('lockNode at not sentOps: ' + lockNode);
                  let next1 = next;
                  next1[2] = key;
                  next1[3] = jsonBody.success.get_objects_resp[0].object_and_clock[0].orset;
                  getObjects('sentOps', type, next1, lockNode);
                }

                break;
            }
            return body;

          })
        });
      });
  });
}

function updateObjects(key, type, op, elements, lockNode) {
  var data = JSON.stringify({
    key: key,
    type: type,
    op: op,
    elements: elements
  });

  let req = http.request({
    host: 'localhost',
    port: 8088,
    path: '/updateObjects',
    method: 'PUT'
  }, function(response) {
    let body = '';
    response.on('data', function(chunk) {
      body += chunk;
    });
    response.on('end', function() {
      //console.log(body);
      let jsonResp = JSON.parse(body);
      //console.log(jsonResp);
      //let commitTime = jsonResp.success.commit_resp.vectorclock[0].dcid_and_time[1];
      //console.log(commitTime);
      //sentOps.push(commitTime);
      //commitTime += "";
      //sentOps[commitTime] = jsonResp;
      //console.log(sentOps);
      if(key.endsWith('_tokens')){
        //console.log('unlocking ' + lockNode);
        unlockNode(lockNode);
      }
      return jsonResp;
    });
  });

  req.write(data);
  req.end();
}

function updateObjectsSnapshot(key, type, op, elements, tokens, vClock, lockNode) {
  //if( ((!(tokens in tokensLegionToAntidote)) && op == 'add')  ||  (tokens in tokensLegionToAntidote && op == 'remove') ) {
    if(!(key in storedObjects)) {
      storedObjects[key] = type;
      //console.log(JSON.stringify(storedObjects));
    }


    let data = JSON.stringify({
      key: key,
      type: type,
      op: op,
      elements: elements,
      vClock: vClock
    });

    var req = http.request({
      host: 'localhost',
      port: 8088,
      path: '/updateObjects',
      method: 'PUT'
    }, function (response) {
      let body = '';
      response.on('data', function (chunk) {
        body += chunk;
      });
      response.on('end', function () {
        //console.log(body);
        let jsonResp = JSON.parse(body);
        //console.log(jsonResp);
        let commitTime = jsonResp.success.commit_resp.vectorclock[0].dcid_and_time[1];
        //console.log(commitTime);
        //lastSeenTimestamp = commitTime + 1;
        //sentOps.push(commitTime);
        //sentOps[commitTime] = jsonResp;
        updateObjects('sentOps', 'crdt_orset', 'add', commitTime);

        lastCommitTimestamp = commitTime;
        //console.log(sentOps);

        //fazer correspondencia de token
        getLogOps(key, type, vClock, ['token', commitTime, tokens, op], lockNode);
        //return jsonResp;
      });
    });

    req.write(data);
    req.end();
}

/******************Log Operations******************/

function getLogOps(key, type, vClock, next, lockNode) {
  //console.log('vlock; ' + vClock);
  //console.log('lockNode at getLogOps: ' + lockNode);
  var data = '/' + key + '/' + type + '/' + JSON.stringify(vClock);
  http.get({
    host: 'localhost',
    port: 8088,
    path: '/getLogOps' + data
  }, function(response) {
    let body = '';
    response.on('data', function(chunk) {
      body += chunk;
    });
    response.on('end', function() {
      //console.log("aqui");
      //console.log(body);
      let jsonResp = JSON.parse(body);
      switch (next[0]) {
        case 'token':
          let ops = jsonResp.success.get_log_operations_resp[0].log_operations;
          let found = false;
          for (i = 0; i < ops.length; i++) {
            if (ops[i].opid_and_payload[1].clocksi_payload[4].commit_time[1] == next[1]) {
              if (next[3] == 'add'){
                //tokensLegionToAntidote[next[2]] = ops[i].opid_and_payload[1].clocksi_payload[2].update.add[1][0].binary64;
                updateObjects(key + '_tokens', 'crdt_orset', 'add', next[2] + ' ' + ops[i].opid_and_payload[1].clocksi_payload[2].update.add[1][0].binary64, lockNode);
              }
              else if (next[3] == 'remove') {
                //delete tokensLegionToAntidote[next[2]];
                updateObjects(key + '_tokens', 'crdt_orset', 'remove', next[2] + ' ' + ops[i].opid_and_payload[1].clocksi_payload[2].update.remove[1][0].binary64, lockNode);
              }
              break;

            }
          }
          break;
        case 'sync':
          //console.log('YO');
          //console.log(JSON.stringify(jsonResp));
          //ver se a operação está nos sendOps, se nao está, fazer no legion
          let logOps = jsonResp.success.get_log_operations_resp[0].log_operations;
          if (Object.keys(logOps).length === 0) {
            getObjects('objectID2', 'crdt_orset', ['updateTime'], lockNode);
            //unlockNode(lockNode);
          }
          else {
            let sentOps = {};
            let tokensLegionToAntidote = {};
            next[2].forEach(function(element, index, array) {
              sentOps[element.element[0].json_value] = element.element[0].json_value;
            });
            next[1].forEach(function(element, index, array) {
              let tokens = element.element[0].json_value.split(" ");
              tokensLegionToAntidote[tokens[0]] = tokens[1];
            });
            //console.log('tokens:');
            //console.log(JSON.stringify(tokensLegionToAntidote));

            //let sentOps = next[4];
            //let tokensLegionToAntidote = next[3];
            Object.keys(logOps).forEach(function (key, index, array) {
              if (!(logOps[key].opid_and_payload[1].clocksi_payload[4].commit_time[1] in sentOps)) {
                console.log('fazer \n' + JSON.stringify(logOps[key].opid_and_payload[1].clocksi_payload[2].update));
                updateObjects('sentOps', type, 'add', logOps[key].opid_and_payload[1].clocksi_payload[4].commit_time[1]);
                //sentOps[logOps[key].opid_and_payload[1].clocksi_payload[4].commit_time[1]] = logOps[key].opid_and_payload;
                let obj = legionDb.getCRDT(logOps[key].opid_and_payload[1].clocksi_payload[0].key.json_value);
                if ('add' in logOps[key].opid_and_payload[1].clocksi_payload[2].update) {
                  obj.add(logOps[key].opid_and_payload[1].clocksi_payload[2].update.add[0].json_value);
                  console.log('OBJ');
                  let opHist = Object.keys(obj.opHistory.map.ObjectServer.map);
                  //console.log(opHist);
                  //console.log(JSON.stringify(obj.opHistory.map.ObjectServer.map[opHist[opHist.length-1]].result.unique));
                  let token = obj.opHistory.map.ObjectServer.map[opHist[opHist.length - 1]].result.unique + " " + logOps[key].opid_and_payload[1].clocksi_payload[2].update.add[1][0].binary64;
                  updateObjects(logOps[key].opid_and_payload[1].clocksi_payload[0].key.json_value + '_tokens', type, 'add', token, lockNode);
                  //tokensLegionToAntidote[obj.opHistory.map.ObjectServer.map[opHist[opHist.length - 1]].result.unique] = logOps[key].opid_and_payload[1].clocksi_payload[2].update.add[1][0].binary64;
                }
                else if ('remove' in logOps[key].opid_and_payload[1].clocksi_payload[2].update) {
                  console.log('fazer \n' + JSON.stringify(logOps[key].opid_and_payload[1].clocksi_payload[2].update));
                  obj.remove(logOps[key].opid_and_payload[1].clocksi_payload[2].update.remove[0].json_value);
                  console.log(JSON.stringify(obj.opHistory.map.ObjectServer.map));
                  let opHist = Object.keys(obj.opHistory.map.ObjectServer.map);
                  console.log('token to remove : ' + obj.opHistory.map.ObjectServer.map[opHist[opHist.length - 1]].result.removes[0]);
                  let token = obj.opHistory.map.ObjectServer.map[opHist[opHist.length - 1]].result.removes[0] + ' ' + tokensLegionToAntidote[obj.opHistory.map.ObjectServer.map[opHist[opHist.length - 1]].result.removes[0]];
                  console.log('token: ' + token);
                  console.log('key: ' + logOps[key].opid_and_payload[1].clocksi_payload[0].key.json_value);
                  updateObjects(logOps[key].opid_and_payload[1].clocksi_payload[0].key.json_value + '_tokens', type, 'remove', token, lockNode);
                  //console.log('token: ' + token);
                  //delete tokensLegionToAntidote[obj.opHistory.map.ObjectServer.map[opHist[opHist.length - 1]].result.removes[0]];
                }
              }
              if (index === array.length - 1)
                lastSeenTimestamp = logOps[key].opid_and_payload[1].clocksi_payload[4].commit_time[1];
                unlockNode(lockNode);
            });
          }
          break;
      }
      return jsonResp;
    });
  });
}

/******************Other Operations******************/

function getLastCommitTimestamp() {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      return xhttp.responseText;
    }
  };

  xhttp.open( "GET", 'http://localhost:8088/getLastCommitTimestamp', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

/******************zooKeeper functions******************/
function deleteNode(path) {
  zooClient.remove(path, -1, function (error) {
    if (error) {
      //console.log(error.stack);
      return;
    }

    //console.log('Node is deleted.');
  });
}

function zooGetChildren(path, callback) {
  let nodes = path.split('/');
  //console.log('path: ' + path);
  //console.log('nodes: ' + nodes);
  zooClient.getChildren('/' + nodes[1], function (error, children, stats) {
    if (error) {
      console.log(error.stack);
      return;
    }
    //console.log('Children are: %j.', children);
    //callback();
    zooGetLock(path, children, callback);
  });
}

function zooGetLock(path, children, callback) {
  let myNumber = path.substr(path.length - 10);
  let lowest = children[0].substr(children[0].length - 10);
  //console.log('myNumber: ' + myNumber);
  children.forEach(function(element) {
    if(parseInt(element.substr(element.length - 10)) < parseInt(lowest))
      lowest = element.substr(element.length - 10)
  });
  //console.log('lowest: ' + lowest);
  if(lowest == myNumber) {
    //console.log('got lock on ' + path);
    let lockNode = path;
    callback(lockNode);
  }
  else {
    zooExistsNotify(path, lowest, callback);
  }
}

function zooExists(path, callback) {
  zooClient.exists(path, function (error, stat) {
    if (error) {
      console.log(error.stack);
      return;
    }

    if (stat) {
      console.log('Node exists.');
    } else {
      console.log('Node does not exist.');
      callback();
    }
  });
}

function zooExistsAndCreate(path, lockNode,callback) {
  if(path.split('/')[1] == 'sentOps') {
    callback(lockNode);
  }
  else {
    zooClient.exists(path, function (error, stat) {
      if (error) {
        console.log(error.stack);
        return;
      }

      if (stat) {
        //console.log('Node exists.');
        if(lockNode != null) {
          callback(lockNode);
        }
        else {
          callback(null);
        }
      } else {
        console.log('Node does not exist. Creating');
        if(lockNode != null) {
          callback(path, lockNode, callback);
        }
        else {
          zooCreate(path, null,callback);
        }
      }
    });
  }
}

function zooExistsNotify(path, lowest,  callback) {
  let watchObj = '/' + path.split('/')[1] + '/' + path.split('/')[1] + lowest;
  //console.log('watching ' + watchObj);
  zooClient.exists(watchObj, function (event) {
    //console.log('Got watcher event: %s', event);
    if(event.getType() == 2 && event.getPath().substr(event.getPath().length - 10) == lowest) {
      //console.log('again ' + path);
      zooGetChildren(path, callback);
    }
  },function (error, stat) {
    if (error) {
      console.log(error.stack);
      return;
    }

    if (stat) {
      //console.log('Node exists. wait for event');
    } else {
      console.log('Node does not exist. That was fast');
      zooGetChildren(path, callback);
    }
  });
}

function zooCreate(path, lockNode ,callback) {
  zooClient.create(
    path,
    new Buffer('data'),
    function (error, path) {
      if (error) {
        console.log(error.stack);
        return;
      }
      //console.log('Node: %s is created.', path);
      callback(lockNode);
    }
  );
}

function zooCreateEfem(path, lockNode, callback) {
  if(path.split('/')[1] == 'sentOps') {
    callback(lockNode);
  }
    else if(lockNode != null) {
    callback(lockNode);
  }
  else {
    zooClient.create(
      path,
      new Buffer('data'),
      zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
      function (error, path) {
        if (error) {
          console.log(error.stack);
          return;
        }
        //console.log('Node: %s is created.', path);
        zooGetChildren(path, callback);
      }
    );
  }
}

function unlockNode(path) {
  zooClient.remove(path, -1, function (error) {
    if (error) {
      //console.log(error.stack);
      return;
    }

    //console.log('node ' + path + ' deleted');
  });
}

/******************fetch updates from antidote from time to time******************/

getObjects('objectID2', 'crdt_orset', ['firstTime'], null);
setTimeout(function(){setInterval(function(){
  Object.keys(storedObjects).forEach(function(key, index) {
    //console.log('key: ' + key + '; value: ' + storedObjects[key]);
    //console.log(lastSeenTimestamp);
    //getLogOps(key, storedObjects[key], [dcid[0], dcid[1], dcid[2], dcid[3], lastSeenTimestamp], ['sync']);
    //usar o de baixo
    getObjects(key, storedObjects[key], ['sync', [dcid[0], dcid[1], dcid[2], dcid[3], lastSeenTimestamp]], null);
    //console.log('sentOps:');
    //console.log(sentOps);
    //console.log('tokensLegionToAntidote:');
    //console.log(JSON.stringify(tokensLegionToAntidote));
  });
}, 4000)}, 4500);


/******************for testing******************/
var counting = 0;
var time1;
var time2;
var timers = [];
var sum = 0;

function checkUpdate(key, type) {
  time1 = new Date();
  let data = '/' + key + '/' + type;
  http.get({
    host: 'localhost',
    port: 8088,
    path: '/getObjects' + data
  }, function(response) {
    let body = '';
    response.on('data', function(chunk) {
      body += chunk;
    });
    response.on('end', function() {
      //console.log(body);
      let jsonBody = JSON.parse(body);
      if(jsonBody.success.get_objects_resp[0].object_and_clock[0].orset.length > counting) {
        counting = jsonBody.success.get_objects_resp[0].object_and_clock[0].orset.length;

        time2 = new Date();
        //let finalTime = time2.getTime() - time1.getTime();
        //timers.push(finalTime);
        //console.log('the op took: ' + finalTime + ' ms');
        //sum += finalTime;
        //let mean = sum / timers.length;
        //console.log('the mean is: ' + mean + ' ms');
        console.log('op ended at: ' + time2.getTime());

        /*let data = JSON.stringify({
          key: key,
          type: type,
          op: 'add',
          elements: counting + '' + counting
        });

        let req = http.request({
          host: 'localhost',
          port: 8088,
          path: '/updateObjects',
          method: 'PUT'
        }, function(response) {
          let body = '';
          response.on('data', function(chunk) {
            body += chunk;
          });
          response.on('end', function() {
            let jsonResp = JSON.parse(body);
            getObjects(key, storedObjects[key], ['sync', [dcid[0], dcid[1], dcid[2], dcid[3], lastSeenTimestamp]], null);
          });
        });

        req.write(data);
        req.end();*/
      }
      else {
        checkUpdate(key, type);
      }


    })
  });
}