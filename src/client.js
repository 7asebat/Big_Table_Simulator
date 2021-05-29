const MASTER_PORT = 3000;
const TABLET1_PORT = 4000;
const TABLET2_PORT = 5000;

let masterSocket = require("socket.io-client")(
  `http://localhost:${MASTER_PORT}`
);
let tablet1Socket = require("socket.io-client")(
  `http://localhost:${TABLET1_PORT}`
);
let tablet2Socket = require("socket.io-client")(
  `http://localhost:${TABLET2_PORT}`
);

let queries = require("./../cases/test1.json");
let metadata = [];

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
  console.log("connected successfully");
  queries.forEach((query) => {
    query.forEach((op) => {
      switch (op.type) {
        case "Set":
          //Handle set queries
          handleSetRequest(op);
          break;

        case "DeleteRow":
          //Handle Delete row queries
          break;

        case "DeleteCells":
          //Handle Delete cells queries
          handleDeleteCellsRequest(op);
          break;

        case "Add":
          //Handle Add queries
          break;

        case "Read":
          //Handle Read queries
          handleReadRequest(op);
          break;
      }
    });
  });
})();

masterSocket.on("metadata", (data) => {
  metadata = data;
  console.log("Received metadata from master\n", metadata);
});

const handleReadRequest = (op) => {
  //Send each query to it's target server
  promises = globalHandler("read", op);
  Promise.all(promises);
};

const handleDeleteCellsRequest = (op) => {
  //Send each query to it's target server
  promises = globalHandler("delete_cells", op);
  Promise.all(promises);
};

const handleSetRequest = (op) => {
  //Send each query to it's target server
  promises = globalHandler("set", op);
  Promise.all(promises);
};

const initQuery = (op) => {
  tabletsKeys =
    op.type == "Read" ? targetServers(op.row_key) : targetServers([op.row_key]);
  serverQueries = [];
  //Separate queries for each tablet
  tabletsKeys.forEach((tabletKeys) => {
    if (tabletKeys.length != 0) {
      tempQuery = Object.assign({}, op);
      tempQuery.row_key = tabletKeys;
      serverQueries.push(tempQuery);
    }
  });
  return serverQueries;
};

const globalHandler = (type, op) => {
  serverQueries = initQuery(op);
  promises = [];
  serverQueries.forEach((q, index) => {
    const tabletSocket = index + 1 == 1 ? tablet1Socket : tablet2Socket;
    promises.push(
      new Promise((resolve) => {
        tabletSocket.emit(`${type}`, q, (res) => {
          message =
            index + 1 == 1
              ? "Result from tablet server 1"
              : "Result from tablet server 2";
          console.log(message, res);
          resolve(res);
        });
      })
    );
  });
  return promises;
};
