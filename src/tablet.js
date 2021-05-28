const MASTER_PORT = 3000;
const TABLET_PORT = process.argv[2];
let socket = require('socket.io-client')(`http://localhost:${MASTER_PORT}`);
const io = require("socket.io")(TABLET_PORT);
let tablets=[]

socket.on("connect", () => {
  socket.send({"type":"tablet"});
});

socket.on("tablets",(data)=>{
  tablets = data;
  // console.log(tablets);
});

io.on("connection", socket => {
  console.log("Client connected ", socket.id);
});


