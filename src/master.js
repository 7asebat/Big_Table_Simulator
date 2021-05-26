const MAX_TABLET_SIZE = 1000;
const bigTable = require("./../assets/users.json");
const partitionData = require("./../utils/partitionData.js");

let tablets = partitionData(bigTable,MAX_TABLET_SIZE);



