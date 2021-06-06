const MAX_TABLET_SIZE = 1000;
const bigTable = require("./../assets/users.json");
const partitionData = require("./../utils/partitionData.js");
const generateMetadata = require("./../utils/generateMetadata.js");
const PORT = 51234;
const io = require("socket.io")(PORT);
let tablets = partitionData(bigTable, MAX_TABLET_SIZE);
let metadata = generateMetadata(tablets, MAX_TABLET_SIZE, bigTable.length);
const { binarySearch } = require("./../utils/binarySearch");
const checkAndDelete = require("../utils/checkFileExist");
const logEvent = require("../utils/logEvent");

let logFile = "./../logs/masterLogs.log";
checkAndDelete(logFile);
logEvent({
  logFile,
  type: "INFO",
  body: `Master has started on port ${PORT}`,
});

let clientConnections = [];
let tabletConnections = [];
//Handling socket connections between services
io.on("connection", (socket) => {
  socket.on("message", (data) => {
    if (data.type == "client") {
      clientConnections.push({
        socket_id: socket.id,
        client_number: clientConnections.length + 1,
      });
      socket.join("client");
      io.to(socket.id).emit("metadata", metadata);
      logEvent({
        logFile,
        type: "INFO",
        body: `Client with socket id = ${socket.id} has connected`,
      });
    } else if (data.type == "tablet") {
      let metaDataIndex = data.port == "51235" ? 0 : 1;
      tabletsIds = metadata[metaDataIndex].tablets_range;
      tabletConnections.push({
        socket_id: socket.id,
        tablet_number: tabletConnections.length + 1,
        data_count: tabletsIds[1] - tabletsIds[0] + 1,
        tablet_port: data.port,
      });
      start = Math.floor(tabletsIds[0] / MAX_TABLET_SIZE);
      end = Math.ceil(tabletsIds[1] / MAX_TABLET_SIZE);
      io.to(socket.id).emit("data", tablets.slice(start, end), MAX_TABLET_SIZE);
      logEvent({
        logFile,
        type: "INFO",
        body: `Tablet with socket id = ${socket.id} has connected`,
      });
    }
  });

  socket.on("disconnect", () => {
    clientConnections = clientConnections.filter(
      (connection) => connection.socket_id !== socket.id
    );
    tabletConnections = tabletConnections.filter(
      (connection) => connection.socket_id !== socket.id
    );
  });

  socket.on("range_update", (tabletsCount, range) => {
    metadata[1].tablets_range[1] = range + MAX_TABLET_SIZE - 1;
    metadata[1].tablets_count = tabletsCount;
    io.to("client").emit("metadata", metadata);
    logEvent({
      logFile,
      type: "INFO",
      body: `Metadata range update - ${JSON.stringify(metadata)}`,
    });
  });

  socket.on(
    "periodic_update",
    (addedData, updatedData, deletedData, dataCount) => {
      updatedElements = [];
      deletedElements = [];
      addedElements = [];
      tabletId = tabletConnections[0].socket_id == socket.id ? 1 : 2;
      tabletConnections[tabletId - 1].data_count = dataCount;
      updatedData.forEach((el) => {
        let { data, index } = binarySearch(
          bigTable,
          el.user_id,
          0,
          bigTable.length - 1
        );
        if (index != -1) {
          bigTable[index] = el;
          updatedElements.push(bigTable[index]);
        }
      });
      deletedData.forEach((el) => {
        let { data, index } = binarySearch(
          bigTable,
          el.user_id,
          0,
          bigTable.length - 1
        );
        if (index != -1) {
          deletedEl = bigTable.splice(index, 1);
          deletedElements.push(deletedEl);
        }
      });
      addedData.forEach((el) => {
        bigTable.push(el);
        addedElements.push(el);
      });

      logEvent({
        logFile,
        type: "INFO",
        body: `Server ${tabletId} periodically updated elements: - ${JSON.stringify(
          updatedElements
        )}`,
      });
      logEvent({
        logFile,
        type: "INFO",
        body: `Server ${tabletId} periodically deleted elements: - ${JSON.stringify(
          deletedElements
        )}`,
      });
      logEvent({
        logFile,
        type: "INFO",
        body: `Server ${tabletId} periodically added elements: - ${JSON.stringify(
          addedElements
        )}`,
      });

      t1DataCount = tabletConnections[0].data_count;
      t2DataCount = tabletConnections[1].data_count;
      if (checkAndReassign(t1DataCount, t2DataCount)) {
        console.log("Reassigning");
        logEvent({
          logFile,
          type: "INFO",
          body: `Repartitioning tablet 1 count ${t1DataCount}, tablet 2 count ${t2DataCount}`,
        });
        handleRepartition();
      }
    }
  );

  //Checks partitioning
  const checkAndReassign = (t1DataCount, t2DataCount) => {
    const reassigningFactor = (1 / 10) * tablets.length;
    if (
      Math.abs(t1DataCount - t2DataCount) >=
      reassigningFactor * MAX_TABLET_SIZE
    ) {
      tablets = partitionData(bigTable, MAX_TABLET_SIZE);
      metadata = generateMetadata(tablets, MAX_TABLET_SIZE, bigTable.length);
      return true;
    }
    return false;
  };

  //Sends data and metadata to tablets and clients
  const handleRepartition = () => {
    tabletConnections.forEach((connection, index) => {
      let metaDataIndex = connection.tablet_port == "51235" ? 0 : 1;
      socketId = connection.socket_id;
      tabletsIds = metadata[metaDataIndex].tablets_range;
      start = Math.floor(tabletsIds[0] / MAX_TABLET_SIZE);
      end = Math.ceil(tabletsIds[1] / MAX_TABLET_SIZE);
      io.to(socketId).emit("partition", tablets.slice(start, end));
    });
    io.to("client").emit("partition", metadata);
  };
});
