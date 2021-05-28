const MASTER_PORT = 3000
const TABLET1_PORT = 4000
const TABLET2_PORT = 5000

let masterSocket = require('socket.io-client')(`http://localhost:${MASTER_PORT}`);
let tablet1Socket = require('socket.io-client')(`http://localhost:${TABLET1_PORT}`);
let tablet2Socket = require('socket.io-client')(`http://localhost:${TABLET2_PORT}`);

let queries = require("./../cases/test1.json");
let metadata=[];


masterSocket.on("connect", () => {
  masterSocket.send({"type":"client"});
});

masterSocket.on("metadata",(data)=>{
  metadata = data;
  console.log("Received metadata from master\n",metadata);
});

tablet1Socket.on("connect",()=>{
  console.log("Connected to tablet 1");
});


tablet2Socket.on("connect",()=>{
  console.log("Connected to tablet 2");
});



