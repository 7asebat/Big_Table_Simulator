module.exports = (arr)=>{
    count = 0;
    arr.forEach(inner=>{
        count+=inner.length;
    });
    return count;
}