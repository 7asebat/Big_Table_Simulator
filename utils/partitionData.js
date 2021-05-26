
module.exports = (data,tabletSize)=>{
    let tablets = [];
    let start = 0;
    let end = tabletSize;
    for(let i = 0;i < Math.ceil(data.length/tabletSize) ;i++,start+=tabletSize,end+=tabletSize)
        tablets.push(data.slice(start,end))
    return tablets;
}