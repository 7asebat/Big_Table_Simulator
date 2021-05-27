
/*  metadata structure
    [{
        tablets_count: number,
        tablets_ids:[],
    }]
*/

module.exports = (tablets)=>{
    const division = Math.floor(tablets.length/2);
    let ids_1=[], ids_2=[]
    for(let i=0;i<tablets.length;i++){
        if(i<division)
            ids_1.push(i);
        else
            ids_2.push(i);
    }
    return [{
        "tablets_count":division,
        "tablets_ids":ids_1
    },
    {
        "tablets_count":tablets.length-division,
        "tablets_ids":ids_2
    }]
}