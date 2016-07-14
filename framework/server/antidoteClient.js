if (typeof exports != "undefined") {
  exports.fetchCounterProto = fetchCounterProto;
  exports.incrementCounterProto = incrementCounterProto;
  exports.treatMessage = treatMessage;
}

const http = require('http');
var dcid = [];
var legionDb;
var firstTime = true;
var sentOps = {};
var tokensLegionToAntidote = {};
var storedObjects = {};
var lastSeenTimestamp = 0;
var lastCommitTimestamp = 0;

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
          switch(message.content.operations[0].opName) {
            case 'add':
              //return updateObjects(message.content .objectID, 'crdt_orset', 'add', message.content.operations[0].result.element);
              return updateObjectsSnapshot(message.content.objectID, 'crdt_orset', 'add', message.content.operations[0].result.element, message.content.operations[0].result.unique, [dcid[0], dcid[1], dcid[2], dcid[3], lastCommitTimestamp]);
              break;
            case 'remove':
              return updateObjectsSnapshot(message.content.objectID, 'crdt_orset', 'remove', message.content.operations[0].result.element, message.content.operations[0].result.removes[0], [dcid[0], dcid[1], dcid[2], dcid[3], lastCommitTimestamp]);
              break;
          }
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

function getObjects(key, type) {
  var data = '/' + key + '/' + type;
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
      console.log(body);
      if(firstTime){
        let jsonBody = JSON.parse(body);
        dcid = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[0].dcid;
        console.log(dcid);
        lastSeenTimestamp = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[1];
        lastCommitTimestamp = jsonBody.success.get_objects_resp[0].object_and_clock[1].vectorclock[0].dcid_and_time[1];
      }
      return body;
    });
  });
}

function updateObjects(key, type, op, elements) {
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
      let commitTime = jsonResp.success.commit_resp.vectorclock[0].dcid_and_time[1];
      //console.log(commitTime);
      //sentOps.push(commitTime);
      //commitTime += "";
      sentOps[commitTime] = jsonResp;
      //console.log(sentOps);
      return jsonResp;
    });
  });

  req.write(data);
  req.end();
}

function updateObjectsSnapshot(key, type, op, elements, tokens, vClock) {
  if( ((!(tokens in tokensLegionToAntidote)) && op == 'add')  ||  (tokens in tokensLegionToAntidote && op == 'remove') ) {
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
        sentOps[commitTime] = jsonResp;
        lastCommitTimestamp = commitTime;
        //console.log(sentOps);

        //fazer correspondencia de token
        getLogOps(key, type, vClock, ['token', commitTime, tokens, op]);
        //return jsonResp;
      });
    });

    req.write(data);
    req.end();
  }
}

/******************Log Operations******************/

function getLogOps(key, type, vClock, next) {
  //console.log('vlock; ' + vClock);
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
          for(i=0; i<ops.length; i++) {
            if(ops[i].opid_and_payload[1].clocksi_payload[4].commit_time[1] == next[1]) {
              if(next[3] == 'add')
                tokensLegionToAntidote[next[2]] = ops[i].opid_and_payload[1].clocksi_payload[2].update.add[1][0].binary64;
              else if(next[3] == 'remove'){
                if(next[2] in tokensLegionToAntidote){
                  delete tokensLegionToAntidote[next[2]];
                }
                else console.log("unique token doesnt exist");
              }
              break;

            }
          }
          console.log(JSON.stringify(tokensLegionToAntidote));
          break;
        case 'sync':
          //console.log('YO');
          console.log(JSON.stringify(jsonResp));
          //ver se a operação está nos sendOps, se nao está, fazer no legion
          let logOps = jsonResp.success.get_log_operations_resp[0].log_operations;
          console.log(sentOps);
          Object.keys(logOps).forEach(function(key, index, array) {
            if( !(logOps[key].opid_and_payload[1].clocksi_payload[4].commit_time[1] in sentOps) ){//guardar tokens
              console.log('fazer \n' + JSON.stringify(logOps[key].opid_and_payload[1].clocksi_payload[2].update));
              let obj = legionDb.getCRDT(logOps[key].opid_and_payload[1].clocksi_payload[0].key.json_value);
              if('add' in logOps[key].opid_and_payload[1].clocksi_payload[2].update) {
                obj.add(logOps[key].opid_and_payload[1].clocksi_payload[2].update.add[0].json_value);

              }
            }
            if(index === array.length - 1)
              lastSeenTimestamp = logOps[key].opid_and_payload[1].clocksi_payload[4].commit_time[1];
          });
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

/******************fetch updates from antidote from time to time******************/

getObjects('objectID2', 'crdt_orset');
setTimeout(function(){setInterval(function(){
  Object.keys(storedObjects).forEach(function(key, index) {
    //console.log('key: ' + key + '; value: ' + storedObjects[key]);
    //console.log(lastSeenTimestamp);
    getLogOps(key, storedObjects[key], [dcid[0], dcid[1], dcid[2], dcid[3], lastSeenTimestamp], ['sync']);
  });
}, 4000)}, 4500);