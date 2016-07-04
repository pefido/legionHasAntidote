if (typeof exports != "undefined") {
  exports.fetchCounterProto = fetchCounterProto;
  exports.incrementCounterProto = incrementCounterProto;
  exports.treatMessage = treatMessage;
}

const http = require('http');
var sentOps = {};

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

function treatMessage(message, db) {

  switch(message.type) {
    case 'OS:C':
      let crdtType = db.getCRDT(message.content.objectID).crdt.type;
      console.log("crdt type: " + crdtType);
      //console.log(message);
      switch(crdtType) {
        case 'STATE_Counter':
          //update state
          break;

        case 'OP_ORSet':
          //console.log(message.content.operations);
          switch(message.content.operations[0].opName) {
            case 'add':
              return updateObjects(message.content.objectID, 'crdt_orset', 'add', message.content.operations[0].result.element);
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
      console.log(jsonResp);
      let commitTime = jsonResp.success.commit_resp.vectorclock[0].dcid_and_time[1];
      console.log(commitTime);
      //sentOps.push(commitTime);
      //commitTime += "";
      sentOps[commitTime] = jsonResp;
      console.log(sentOps);
      return jsonResp;
    });
  });

  req.write(data);
  req.end();
}

/******************Log Operations******************/

function getLogOps(key, type, vClock) {
  var data = '/' + key + '/' + type + '/' + vClock;
  http.get({
    host: 'localhost',
    port: 8088,
    path: '//getLogOps' + data
  }, function(response) {
    let body = '';
    response.on('data', function(chunk) {
      body += chunk;
    });
    response.on('end', function() {
      console.log(body);
      return body;
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

/******************fetch updates from antidotes from time to time******************/

setTimeout(function(){setInterval(function(){

}, 2000)}, 4500);