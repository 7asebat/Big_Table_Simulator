const MASTER_PORT = 3000;
const TABLET_PORT = process.argv[2];
let socket = require("socket.io-client")(`http://localhost:${MASTER_PORT}`);
const io = require("socket.io")(TABLET_PORT);
const { binarySearch } = require("./../utils/binarySearch");
const count2d = require("./../utils/countArr");
let tablets = [];
//Holds data to be updated periodically
let updatedData = [];
let deletedData = [];

//Determine currently used table
let lockedTabletId = -1;

const acquireLock = (tabletId)=>{
  console.log("Acquiring lock");
  while(lockedTabletId!=-1);
  console.log("Acquired Lock");
  lockedTabletId = tabletId;
}

const releaseLock = ()=>{
  lockedTabletId = -1;
}

//Send an event to master server to update main table
setInterval(function () {
  if (updatedData.length || deletedData.length) {
    dataCount = count2d(tablets);
    console.log("Current data count = ",dataCount);
    socket.emit("periodic_update", updatedData, deletedData,dataCount);
    updatedData = [];
    deletedData=[];
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

socket.on("partition", (data) => {
  console.log("Received partition data");
  tablets = data;
});

socket.on("data",(data)=>{
  tablets = data;
  console.log("Received initial data", data);
});

io.on("connection", (socket) => {
  console.log("Client connected ", socket.id);

  socket.on("read", (q, cb) => {
    console.log(
      "Received read request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = requestHandler("Read", q);
    cb(results);
  });

  socket.on("delete_cells", (q, cb) => {
    console.log(
      "Received delete cells request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = requestHandler("DeleteCells", q);
    cb(results);
  });

  socket.on("delete_row", (q, cb) => {
    console.log(
      "Received delete row request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = requestHandler("DeleteRow", q);
    cb(results);
  });

  socket.on("set", (q, cb) => {
    console.log(
      "Received set request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = requestHandler("Set", q);
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
    if (tablet_id == -1) results.push(`row with user_id = ${key} wasn't found`);
    else {
      let { data, index } = binarySearch(
        tablets[tablet_id],
        key,
        0,
        tablets[tablet_id].length - 1
      );
      switch (type) {
        case "Set":
          acquireLock(tablet_id);
          if (index == -1)
            results.push(`row with user_id = ${key} wasn't found`);
          else {
            Object.entries(q.columns_data).forEach(([key, value]) => {
              tablets[tablet_id][index][`${key}`] = value;
            });
            if (!existsInUpdatedData(data.user_id)) updatedData.push(data);
            results.push(data);
          }
          releaseLock();
          break;

        case "DeleteCells":
          acquireLock(tablet_id);
          if (index == -1)
            results.push(`row with user_id = ${key} wasn't found`);
          else {
            q.columns.forEach((column) => {
              tablets[tablet_id][index][`${column}`] = null;
            });
            if (!existsInUpdatedData(data.user_id)) updatedData.push(data);
            results.push(data);
          }
          releaseLock();
          break;

        case "Read":
          if (index == -1)
            results.push(`row with user_id = ${key} wasn't found`);
          else results.push(data);
          break;

        case "DeleteRow":
          acquireLock(tablet_id);
          if (index == -1)
            results.push(`row with user_id = ${key} wasn't found`);
          else {
            deletedEl = tablets[tablet_id].splice(index, 1);
            console.log("Deleted: ", deletedEl);
            results.push(`Entry with key = ${key} is deleted successfully`);
            deletedData.push(data);
          }
          releaseLock();
          break;
      }
    }
  });
  return results;
};
