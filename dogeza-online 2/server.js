/*
 * 土下座メーター - オンライン対戦サーバー
 *
 * 使い方:
 *   npm install
 *   npm start
 *
 * このPC自身でテストするなら http://localhost:3000 でOKです。
 *
 * 別端末（同じWi-Fi内）から「カメラ判定モード」も使ってもらう場合は、
 * 表示される https://<このPCのLAN IP>:3443 を使ってください。
 * ブラウザの「カメラ利用」は https か localhost でしか許可されないため、
 * 自己署名証明書で簡易HTTPSサーバーも同時に立てています。
 * 初回アクセス時に「この接続はプライベートではありません」といった警告が
 * 出ますが、自分で立てたサーバーなので「詳細設定」→「アクセスする」で
 * 進んで問題ありません。
 *
 * マウスモードだけで良ければ http://<LAN IP>:3000 でも参加できます。
 *
 * ルームコードで2人をマッチングし、進捗と結果をWebSocketで中継するだけの
 * シンプルなリレーサーバーです。ゲームの判定・演出はすべてクライアント側
 * （public/index.html）で行われます。http/https どちらの入口から入っても
 * 同じ「rooms」データを共有しているので、ホストとゲストが別プロトコルで
 * 参加していても問題なくマッチングできます。
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');
const WebSocket = require('ws');

let selfsigned = null;
try {
  selfsigned = require('selfsigned');
} catch (e) {
  selfsigned = null;
}

const HTTP_PORT = process.env.PORT || 3000;
const HTTPS_PORT = process.env.HTTPS_PORT || 3443;
const PUBLIC_DIR = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.webp': 'image/webp',
  '.mp3': 'audio/mpeg',
  '.wav': 'audio/wav',
  '.ogg': 'audio/ogg'
};

function requestHandler(req, res) {
  let reqPath = decodeURIComponent(req.url.split('?')[0]);
  if (reqPath === '/') reqPath = '/index.html';
  const filePath = path.join(PUBLIC_DIR, reqPath);

  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
}

function listLanIps() {
  const nets = os.networkInterfaces();
  const results = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) results.push(net.address);
    }
  }
  return results;
}

// ---------- room / relay logic (http・https共通) ----------

// roomCode -> { host, guest, hostName, guestName, hostResult, guestResult }
const rooms = new Map();

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 紛らわしい 0/O, 1/I は除外

function generateCode() {
  let code;
  do {
    code = '';
    for (let i = 0; i < 4; i++) {
      code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    }
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function otherPlayer(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return null;
  return ws.role === 'host' ? room.guest : room.host;
}

function cleanupRoom(ws) {
  if (!ws.roomCode) return;
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const other = ws.role === 'host' ? room.guest : room.host;
  send(other, { type: 'opponent_left' });
  rooms.delete(ws.roomCode);
}

function handleConnection(ws) {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return;
    }

    if (msg.type === 'create_room') {
      const code = generateCode();
      rooms.set(code, {
        host: ws,
        guest: null,
        hostName: (msg.name || 'ホスト').slice(0, 20),
        guestName: null,
        hostResult: null,
        guestResult: null,
        subMode: msg.subMode === 'score' ? 'score' : 'hp' // ホストが選んだ対戦タイプ（HPバトル or スコア対戦）
      });
      ws.roomCode = code;
      ws.role = 'host';
      send(ws, { type: 'room_created', code });
      return;
    }

    if (msg.type === 'join_room') {
      const code = String(msg.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) {
        send(ws, { type: 'error', message: 'ルームが見つかりません。コードを確認してください。' });
        return;
      }
      if (room.guest) {
        send(ws, { type: 'error', message: 'このルームはすでに満員です。' });
        return;
      }
      room.guest = ws;
      room.guestName = (msg.name || 'ゲスト').slice(0, 20);
      ws.roomCode = code;
      ws.role = 'guest';
      send(ws, { type: 'joined', code, opponentName: room.hostName, subMode: room.subMode });
      send(room.host, { type: 'opponent_joined', opponentName: room.guestName });
      return;
    }

    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    const other = otherPlayer(ws);

    if (msg.type === 'progress') {
      send(other, {
        type: 'opponent_progress',
        angle: msg.angle,
        roundIndex: msg.roundIndex,
        holdPct: msg.holdPct,
        dealtDamage: msg.dealtDamage,
        hp: msg.hp
      });
      return;
    }

    // バトルモード：どちらかのHPが0になった側が中継サーバーに敗北を通知
    if (msg.type === 'ko') {
      send(other, { type: 'opponent_ko' });
      return;
    }

    // WebRTC signaling (映像通話用) - サーバーはそのまま中継するだけ
    if (msg.type === 'rtc_offer') {
      send(other, { type: 'rtc_offer', sdp: msg.sdp });
      return;
    }
    if (msg.type === 'rtc_answer') {
      send(other, { type: 'rtc_answer', sdp: msg.sdp });
      return;
    }
    if (msg.type === 'rtc_ice') {
      send(other, { type: 'rtc_ice', candidate: msg.candidate });
      return;
    }

    if (msg.type === 'finished') {
      if (ws.role === 'host') room.hostResult = msg.result;
      else room.guestResult = msg.result;

      send(other, { type: 'opponent_finished', result: msg.result });

      if (room.hostResult && room.guestResult) {
        const payload = {
          type: 'both_finished',
          host: room.hostResult,
          guest: room.guestResult,
          hostName: room.hostName,
          guestName: room.guestName
        };
        send(room.host, payload);
        send(room.guest, payload);
      }
      return;
    }

    if (msg.type === 'leave') {
      cleanupRoom(ws);
      return;
    }
  });

  ws.on('close', () => {
    cleanupRoom(ws);
  });
}

function attachHeartbeat(wss) {
  const heartbeat = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) return ws.terminate();
      ws.isAlive = false;
      ws.ping();
    });
  }, 30000);
  wss.on('close', () => clearInterval(heartbeat));
}

// ---------- HTTPサーバー（このPC上のテスト・マウスモードでのLAN参加用）----------

const httpServer = http.createServer(requestHandler);
const wssHttp = new WebSocket.Server({ server: httpServer });
wssHttp.on('connection', handleConnection);
attachHeartbeat(wssHttp);

// ---------- HTTPSサーバー（LAN内の別端末でカメラ判定を使うために必要）----------

let httpsServer = null;

function buildSelfSignedCert() {
  const lanIps = listLanIps();
  const altNames = [
    { type: 2, value: 'localhost' }, // DNS
    { type: 7, ip: '127.0.0.1' } // IP
  ];
  lanIps.forEach((ip) => altNames.push({ type: 7, ip }));

  const attrs = [{ name: 'commonName', value: 'dogeza-online.local' }];
  const pems = selfsigned.generate(attrs, {
    days: 825,
    keySize: 2048,
    extensions: [{ name: 'subjectAltName', altNames }]
  });
  return { key: pems.private, cert: pems.cert };
}

function startHttpsServer() {
  if (!selfsigned) {
    console.log('  ⚠ "selfsigned" パッケージが見つからないため、HTTPSサーバーは起動しません。');
    console.log('    npm install を実行してから再度お試しください。');
    return;
  }
  try {
    const { key, cert } = buildSelfSignedCert();
    httpsServer = https.createServer({ key, cert }, requestHandler);
    const wssHttps = new WebSocket.Server({ server: httpsServer });
    wssHttps.on('connection', handleConnection);
    attachHeartbeat(wssHttps);

    httpsServer.listen(HTTPS_PORT, () => {
      printStartupInfo();
    });
  } catch (e) {
    console.log('  ⚠ HTTPSサーバーの起動に失敗しました:', e.message);
    console.log('    カメラ判定モードはlocalhostでのみ利用可能です。');
  }
}

// ---------- 起動 ----------

let printedOnce = false;
function printStartupInfo() {
  if (printedOnce) return;
  printedOnce = true;

  const ips = listLanIps();
  console.log('');
  console.log('  土下座メーター 対戦サーバー起動！（カメラ判定のみ）');
  console.log('');
  console.log('  [このPCで開く]');
  console.log('    http://localhost:' + HTTP_PORT);
  console.log('');
  if (ips.length) {
    if (httpsServer) {
      console.log('  [他の端末から参加（カメラを使うには必ずこちら）]');
      ips.forEach((ip) => console.log('    https://' + ip + ':' + HTTPS_PORT));
      console.log('    ↑初回は証明書の警告が出ますが「詳細設定」→「アクセスする」でOKです。');
      console.log('');
    } else {
      console.log('  ⚠ HTTPSサーバーが起動していないため、他の端末からはカメラを利用できません。');
      console.log('');
    }
  } else {
    console.log('  （LAN IPが見つかりませんでした。同じWi-Fiに接続しているか確認してください）');
    console.log('');
  }
  console.log('  ※このアプリはカメラ判定のみです。ブラウザの制約上 https か localhost でしか動きません。');
  console.log('');
}

httpServer.listen(HTTP_PORT, () => {
  startHttpsServer();
  // HTTPSが立たない/失敗した場合でも案内は出す
  setTimeout(printStartupInfo, 300);
});
