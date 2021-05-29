const MASTER_PORT = 3000;
const MAX_TABLET_SIZE = 1000;
const TABLET_PORT = process.argv[2];
let socket = require("socket.io-client")(`http://localhost:${MASTER_PORT}`);
const io = require("socket.io")(TABLET_PORT);
const {binarySearch} = require("./../utils/binarySearch");

const getTabletIndex = (row_key) => {
  let tabletIndex = -1;
  tablets.forEach((tablet, index) => {
    if (
      tablet[0].user_id <= row_key &&
      tablet[tablet.length - 1].user_id >= row_key
    )
      tabletIndex = index;
  });
  return tabletIndex;
};

socket.on("connect", () => {
  socket.send({ type: "tablet" });
});

socket.on("tablets", (data) => {
  tablets = data;
});

io.on("connection", (socket) => {
  console.log("Client connected ", socket.id);

  socket.on("Read", (q,cb) => {
    console.log("Received read request from client with socket id = ", socket.id);
    console.log(q);
    results = []
    q.row_key.forEach((key) => {
      tablet_id = getTabletIndex(key);
      results.push(binarySearch(tablets[tablet_id],key,0,tablets[tablet_id].length));
    });
    cb(results);
  });
});
