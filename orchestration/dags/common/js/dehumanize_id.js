var public_id = id.replace(/8/g, 'O').replace(/9/g, 'I');
var byteArray = new Uint8Array(base32Decode(public_id, 'RFC4648')).reverse();
var value = 0;
for (var i = byteArray.length - 1; i >= 0; i--) {
    value = (value * 256) + byteArray[i];
}
return value;
