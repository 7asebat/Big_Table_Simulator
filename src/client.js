const PORT = 3000
let socket = require('socket.io-client')(`http://localhost:${PORT}`);
let metadata=[];

socket.on("connect", () => {
  socket.send({"type":"client"});
});

socket.on("metadata",(data)=>{
  metadata = data;
  console.log("Received metadata from master\n",metadata);
});

socket.on("Testing broadcasting",()=>{
  console.log("Received msg");
})