const PORT = 3000
let socket = require('socket.io-client')(`http://localhost:${PORT}`);
let tablets=[]
socket.on("connect", () => {
  socket.send({"type":"tablet"});
});

socket.on("tablets",(data)=>{
  tablets = data;
  console.log(tablets);
})  