const MASTER_PORT = 3000;
const TABLET_PORT = process.argv[2];
let socket = require("socket.io-client")(`http://localhost:${MASTER_PORT}`);
const io = require("socket.io")(TABLET_PORT);
const { binarySearch } = require("./../utils/binarySearch");

//Holds data ids to be updated periodically
let updatedData = [];

//Send an event to master server to update main table
setInterval(function () {
  if(updatedData.length){
    socket.emit("periodic_update", updatedData);
    updatedData = [];
  }
}, 60 * 1000); // 60 * 1000 milsec

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

  socket.on("read", (q, cb) => {
    console.log(
      "Received read request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = requestHandler("Read",q);
    cb(results);
  });

  socket.on("delete_cells", (q, cb) => {
    console.log(
      "Received delete cells request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = requestHandler("DeleteCells",q);
    cb(results);
  });

  socket.on("set", (q, cb) => {
    console.log(
      "Received set request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = requestHandler("Set",q);
    cb(results);
  });
});

const existsInUpdatedData = (id) => {
  found = updatedData.findIndex((el) => el.user_id == id) !== -1 ? true : false;
  return found;
};

const requestHandler = (type, q) => {
  results = [];
  q.row_key.forEach((key) => {
    tablet_id = getTabletIndex(key);
    let { data, index } = binarySearch(
      tablets[tablet_id],
      key,
      0,
      tablets[tablet_id].length - 1
    );
    switch (type) {
      case "Set":
        Object.entries(q.columns_data).forEach(([key, value]) => {
          tablets[tablet_id][index][`${key}`] = value;
        });
        if (!existsInUpdatedData(data.user_id)) updatedData.push(data);
        results.push(data);
        break;

      case "DeleteCells":
        q.columns.forEach((column) => {
          tablets[tablet_id][index][`${column}`] = null;
        });
        if (!existsInUpdatedData(data.user_id)) updatedData.push(data);
        results.push(data);
        break;

      case "Read":
        if (index == -1) results.push(`row with user_id = ${key} wasn't found`);
        else results.push(data);
        break;
    }
  });
  return results;
};
