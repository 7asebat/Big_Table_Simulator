const fs = require("fs");

const logEvent = function ({ logFile, type, body }) {
  const line = `[${new Date().toISOString()}][${type}] \t-\t ${body}\n`;
  fs.appendFileSync(logFile, line);
};

module.exports = logEvent;
