const MASTER_PORT = 51234;
const TABLET1_PORT = 51235;
const TABLET2_PORT = 51236;
const MASTER_IP = process.argv[2];
const TABLET1_IP = process.argv[3];
const TABLET2_IP = process.argv[4];
const testCase = process.argv[5];
const sleepTime = process.argv[6];

const logEvent = require("../utils/logEvent");
const checkAndDelete = require("../utils/checkFileExist");
let masterSocket = require("socket.io-client")(
  `http://${MASTER_IP}:${MASTER_PORT}`
);
let tablet1Socket = require("socket.io-client")(
  `http://${TABLET1_IP}:${TABLET1_PORT}`
);
let tablet2Socket = require("socket.io-client")(
  `http://${TABLET2_IP}:${TABLET2_PORT}`
);

let queries = require(`./../cases/${testCase}.json`);
let metadata = [];

let logFile = "./../logs/clientLogs.log";

checkAndDelete(logFile);
logEvent({
  logFile,
  type: "INFO",
  body: `Client has started`,
});

const init = async () => {
  const connections = [masterSocket, tablet1Socket, tablet2Socket];
  const promises = connections.map((connection) => {
    return new Promise((resolve, reject) => {
      connection.on("connect", () => {
        if (connection == masterSocket) masterSocket.send({ type: "client" });
        resolve();
      });
    });
  });

  return Promise.all(promises);
};

const targetServers = (keys) => {
  tablet1Keys = [];
  tablet2Keys = [];
  keys.forEach((key) => {
    metadata.forEach((entry, index) => {
      if (key >= entry.tablets_range[0] && key <= entry.tablets_range[1])
        index + 1 == 1 ? tablet1Keys.push(key) : tablet2Keys.push(key);
    });
  });

  return [tablet1Keys, tablet2Keys];
};

(async () => {
  await init();
  logEvent({
    logFile,
    type: "INFO",
    body: `Client has connected to Tablet on http://${TABLET1_IP}:${TABLET1_PORT} and on http://${TABLET2_IP}:${TABLET2_PORT} and to Master on http://${MASTER_IP}:${MASTER_PORT}`,
  });

  masterSocket.on("partition", (data) => {
    logEvent({
      logFile,
      type: "INFO",
      body: `Received initial metadata\t-\t${JSON.stringify(data)}`,
    });
    metadata = data;
  });

  queries.forEach((query, index) => {
    setTimeout(function () {
      switch (query.type) {
        case "Set":
          //Handle set queries
          handleSetRequest(query);
          break;

        case "DeleteRow":
          //Handle Delete row queries
          handleDeleteRowRequest(query);
          break;

        case "DeleteCells":
          //Handle Delete cells queries
          handleDeleteCellsRequest(query);
          break;

        case "AddRow":
          //Handle Add queries
          handleAddRequest(query);
          break;

        case "Read":
          //Handle Read queries
          handleReadRequest(query);
          break;
      }
    }, index * sleepTime);
  });
})();

masterSocket.on("metadata", (data) => {
  logEvent({
    logFile,
    type: "INFO",
    body: `Received new metadata\t-\t${JSON.stringify(data)}`,
  });
  metadata = data;
});

const handleReadRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("read", query);
  Promise.all(promises);
};

const handleDeleteCellsRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("delete_cells", query);
  Promise.all(promises);
};

const handleSetRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("set", query);
  Promise.all(promises);
};

const handleAddRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("add_row", query);
  Promise.all(promises);
};

const handleDeleteRowRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("delete_row", query);
  Promise.all(promises);
};

const initQuery = (query) => {
  tablet1Queries = {};
  tablet2Queries = {};
  if (query.type == "AddRow") {
    let tempQuery = Object.assign({}, query);
    tempQuery.row_key = [query.row_key];
    Object.assign(tablet2Queries, tempQuery);
  } else {
    tabletsKeys =
      query.type == "Read" || query.type == "DeleteRow"
        ? targetServers(query.row_key)
        : targetServers([query.row_key]);

    //Separate queries for each tablet
    tabletsKeys.forEach((tabletKeys, index) => {
      if (tabletKeys.length != 0) {
        tempQuery = Object.assign({}, query);
        tempQuery.row_key = tabletKeys;
        index == 0
          ? Object.assign(tablet1Queries, tempQuery)
          : Object.assign(tablet2Queries, tempQuery);
      }
    });
  }
  return [tablet1Queries, tablet2Queries];
};

const globalHandler = (type, query) => {
  serverQueries = initQuery(query);
  promises = [];
  serverQueries.forEach((q, index) => {
    if (!isEmptyObject(q)) {
      const tabletSocket = index + 1 == 1 ? tablet1Socket : tablet2Socket;
      promises.push(
        new Promise((resolve) => {
          tabletSocket.emit(`${type}`, q, (res) => {
            message =
              index + 1 == 1
                ? "Result from tablet server 1"
                : "Result from tablet server 2";
            logEvent({
              logFile,
              type: "QUERY",
              body: `Executing query: ${JSON.stringify(
                q
              )} \t-\t Query Result: ${JSON.stringify(res)}`,
            });
            resolve(res);
          });
        })
      );
    }
  });
  return promises;
};

function isEmptyObject(obj) {
  return !Object.keys(obj).length;
}
