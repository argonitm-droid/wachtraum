import http from 'node:http';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {randomInt} from 'node:crypto';
import {WebSocketServer,WebSocket} from 'ws';
import {pathToFileURL} from 'node:url';
export function createSignalingServer({port=Number(process.env.PORT||8080),host='0.0.0.0',staticDir=null,origins=(process.env.ALLOWED_ORIGINS||'http://localhost:8060,http://127.0.0.1:8060').split(',')}={}) {
 const rooms=new Map();const clients=new Set();
 const server=http.createServer(async(req,res)=>{
  if(req.url==='/health'){res.writeHead(200,{'Content-Type':'application/json'});res.end('{"ok":true}');return;}
  if(staticDir){try{let name=decodeURIComponent(new URL(req.url,'http://local').pathname);if(name==='/')name='/index.html';const file=path.resolve(staticDir,'.'+name);if(!file.startsWith(path.resolve(staticDir)+path.sep))throw Error();const bytes=await readFile(file);const types={'.wasm':'application/wasm','.js':'text/javascript','.html':'text/html','.json':'application/json','.png':'image/png'};res.writeHead(200,{'Content-Type':types[path.extname(file)]||'application/octet-stream','Cache-Control':'no-store'});res.end(bytes);return;}catch{}}
  res.writeHead(404);res.end();
 });
 const wss=new WebSocketServer({noServer:true,maxPayload:65536,perMessageDeflate:false});
 const send=(s,m)=>{if(s?.readyState===WebSocket.OPEN)s.send(JSON.stringify(m));};
 const error=(s,key)=>send(s,{op:'error',key});
 server.on('upgrade',(req,socket,head)=>{
  if(!origins.includes(req.headers.origin)||clients.size>=200){socket.end('HTTP/1.1 403 Forbidden\r\n\r\n');return;}
  wss.handleUpgrade(req,socket,head,s=>wss.emit('connection',s,req));
 });
 function leave(s){
  clients.delete(s);const room=rooms.get(s.room);if(!room)return;
  if(s.peer===1){rooms.delete(s.room);send(room.guest,{op:'host_left'});room.guest?.close(1000);}
  else if(room.guest===s){room.guest=null;send(room.host,{op:'guest_left'});}
 }
 wss.on('connection',s=>{
  clients.add(s);s.alive=true;s.count=0;s.window=Date.now();s.on('pong',()=>s.alive=true);
  s.on('message',raw=>{
   if(Date.now()-s.window>10000){s.window=Date.now();s.count=0;}
   if(++s.count>150){s.close(1008);return;}
   let m;try{m=JSON.parse(raw);}catch{error(s,'NET_BAD');return;}
   if(!m||typeof m!=='object'){error(s,'NET_BAD');return;}
   if(m.op==='create'&&!s.room){
    if(rooms.size>=100){error(s,'NET_FULL');return;}
    let code;const alphabet='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';do{code=Array.from({length:6},()=>alphabet[randomInt(alphabet.length)]).join('');}while(rooms.has(code));
    s.room=code;s.peer=1;rooms.set(code,{host:s,guest:null});send(s,{op:'room',code,peer:1});
   }else if(m.op==='join'&&!s.room){
    if(typeof m.code!=='string'||! /^[A-Z2-9]{6}$/.test(m.code)){error(s,'NET_CODE');return;}
    const r=rooms.get(m.code);if(!r){error(s,'NET_MISSING');return;}if(r.guest){error(s,'NET_FULL');return;}
    s.room=m.code;s.peer=2;r.guest=s;send(s,{op:'room',code:m.code,peer:2});send(r.host,{op:'ready'});
   }else if(['offer','answer','ice'].includes(m.op)&&s.room){
    if(m.op==='offer'&&s.peer!==1||m.op==='answer'&&s.peer!==2){error(s,'NET_BAD');return;}
    if(m.op==='ice'&&(typeof m.mid!=='string'||!Number.isInteger(m.index)||m.index<0||m.index>32||typeof m.candidate!=='string'||m.candidate.length>4096)){error(s,'NET_BAD');return;}
    if(m.op!=='ice'&&(typeof m.sdp!=='string'||m.sdp.length>60000)){error(s,'NET_BAD');return;}
    const r=rooms.get(s.room);if(!r)return;
    send(s.peer===1?r.guest:r.host,m.op==='ice'?{op:'ice',mid:m.mid,index:m.index,candidate:m.candidate}:{op:m.op,sdp:m.sdp});
   }else error(s,'NET_BAD');
  });s.on('close',()=>leave(s));s.on('error',()=>{});
 });
 const heartbeat=setInterval(()=>{for(const s of clients){if(!s.alive){s.terminate();continue;}s.alive=false;s.ping();}},15000);heartbeat.unref();
 server.listen(port,host);
 return {server,rooms,wss,close:()=>{clearInterval(heartbeat);for(const s of clients)s.terminate();wss.close();server.close();}};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)createSignalingServer();
