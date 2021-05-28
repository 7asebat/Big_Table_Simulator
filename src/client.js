const MASTER_PORT = 3000
const TABLET1_PORT = 4000
const TABLET2_PORT = 5000

let masterSocket = require('socket.io-client')(`http://localhost:${MASTER_PORT}`);
let tablet1Socket = require('socket.io-client')(`http://localhost:${TABLET1_PORT}`);
let tablet2Socket = require('socket.io-client')(`http://localhost:${TABLET2_PORT}`);

let queries = require("./../cases/test1.json");
let metadata=[];

const init = async ()=>{
  const connections = [masterSocket, tablet1Socket, tablet2Socket];
  const promises = connections.map(connection => {
    return new Promise((resolve, reject) => {
      connection.on("connect", () => {
        if(connection == masterSocket)
          masterSocket.send({"type":"client"});
        resolve();
      });  
    })
  });
  
  return Promise.all(promises);
};

(async() => {
  await init();
  console.log("connected successfully");
})();

masterSocket.on("metadata",(data)=>{
  metadata = data;
  console.log("Received metadata from master\n",metadata);
});








