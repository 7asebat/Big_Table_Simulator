const fs = require("fs");
var path = require('path');

let csvFileName = process.argv[2]
let jsonFileName = process.argv[3]
csv = fs.readFileSync(path.join(".","..","assets",csvFileName))

let rows = csv.toString().split("\r\n");
let keys=[]
let jsonObjects = []
i=1;
rows.forEach((row,index)=>{
    if(index==0){
        keys = row.split(',')
    }
    else{
        let vals = row.split(',')
        let entry = {}
        keys.forEach((key,index)=>{
            if(key == "user_id"){
                entry[key] = i;
            }
            else{
            entry[key] = vals[index]
            }
        })
        jsonObjects.push(entry)
        i++
    }
});

fs.writeFileSync(path.join(".","..","assets",jsonFileName),JSON.stringify(jsonObjects));