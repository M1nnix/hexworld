#!/usr/bin/env python3
"""Hex World public WebSocket relay + HTML host.
Install: pip install -r requirements.txt
Run: python server.py
Deploy this process behind an HTTPS public hostname for world-wide multiplayer.
"""
import asyncio
import json
import logging
import os
import re
import secrets
import time
from pathlib import Path
from aiohttp import web, WSMsgType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger('hex-world-relay')
ROOT = Path(__file__).resolve().parent
GAME_HTML = ROOT / 'index.html'
PORT = int(os.environ.get('PORT', '3000'))
MAX_PEERS = max(2, int(os.environ.get('MAX_PEERS', '300')))
MAX_LINKS_PER_PEER = 12
MAX_MESSAGE_BYTES = 512_000
ID_RE = re.compile(r'^hexworld-[0-9]{5}$')

# All registrations and links are per process. Use a SINGLE server instance.
clients = {}  # peer ID -> browser WebSocket
links = {}  # link -> (initiating peer ID, target peer ID)

async def send(ws, payload):
    if ws and not ws.closed:
        try:
            await ws.send_json(payload)
            return True
        except (ConnectionResetError, RuntimeError):
            return False
    return False

async def close_link(key):
    pair = links.pop(key, None)
    if not pair:
        return
    for client_id in pair:
        await send(clients.get(client_id), {'op': 'LINK_CLOSED', 'link': key})

async def unregister(peer_id, ws):
    if peer_id and clients.get(peer_id) is ws:
        del clients[peer_id]
        for link in [k for k, pair in links.items() if peer_id in pair]:
            await close_link(link)
        LOG.info('Disconnected %s (%s peers remaining)', peer_id[:24], len(clients))

async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=MAX_MESSAGE_BYTES,
                               receive_timeout=90)
    await ws.prepare(request)
    peer_id = None
    messages_in_window = 0
    window_start = time.monotonic()
    try:
        async for incoming in ws:
            if incoming.type != WSMsgType.TEXT:
                if incoming.type in (WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSING):
                    break
                continue
            if time.monotonic()-window_start>10:
                window_start=time.monotonic()
                messages_in_window=0
            messages_in_window+=1
            if messages_in_window>250:
                await ws.close(code=1008, message=b'Too many messages')
                break
            try:
                msg = json.loads(incoming.data)
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(msg, dict):
                continue
            op = msg.get('op')
            if op == 'REGISTER':
                if peer_id:
                    continue
                wanted = msg.get('id')
                if wanted is None:
                    wanted = 'guest-' + secrets.token_hex(12)
                if not isinstance(wanted, str) or not (ID_RE.fullmatch(wanted) or re.fullmatch(r'guest-[a-f0-9]{24}', wanted)):
                    await send(ws, {'op': 'ERROR', 'type': 'invalid-id'})
                    await ws.close(code=1008)
                    break
                if wanted in clients:
                    await send(ws, {'op': 'ERROR', 'type': 'unavailable-id'})
                    await ws.close(code=1008)
                    break
                if len(clients)>=MAX_PEERS:
                    await send(ws, {'op': 'ERROR', 'type': 'server-full'})
                    await ws.close(code=1013)
                    break
                clients[wanted]=ws
                peer_id=wanted
                await send(ws, {'op': 'OPEN', 'id': peer_id})
                LOG.info('Registered %s', peer_id[:24])
                continue
            if not peer_id:
                await send(ws, {'op': 'ERROR', 'type': 'not-registered'})
                continue
            if op == 'CONNECT':
                to = msg.get('target')
                req = msg.get('request')
                if not isinstance(req, str) or len(req)>70 or not isinstance(to,str) or not ID_RE.fullmatch(to):
                    await send(ws, {'op': 'ERROR', 'type':'invalid-id', 'request':req})
                    continue
                target_ws = clients.get(to)
                if not target_ws or target_ws.closed or to == peer_id:
                    await send(ws, {'op': 'ERROR', 'type':'peer-unavailable', 'request':req})
                    continue
                used_links = sum(peer_id in pair for pair in links.values())
                if used_links>=MAX_LINKS_PER_PEER:
                    await send(ws, {'op': 'ERROR', 'type':'connection-limit', 'request':req})
                    continue
                link = secrets.token_urlsafe(18)
                links[link] = (peer_id, to)
                if not await send(target_ws, {'op': 'INCOMING', 'link':link, 'from':peer_id}):
                    links.pop(link,None)
                    await send(ws, {'op': 'ERROR','type':'peer-unavailable','request':req})
                    continue
                await send(target_ws, {'op': 'LINK_OPEN', 'link':link})
                await send(ws, {'op': 'LINK_OPEN', 'link':link, 'request':req, 'peer':to})
                continue
            if op == 'DATA':
                link = msg.get('link')
                pair = links.get(link)
                if not pair or peer_id not in pair:
                    continue
                other = pair[1] if pair[0]==peer_id else pair[0]
                await send(clients.get(other), {'op':'DATA','link':link,'data':msg.get('data')})
                continue
            if op == 'CLOSE_LINK':
                link = msg.get('link')
                if link in links and peer_id in links[link]:
                    await close_link(link)
                continue
    except (asyncio.TimeoutError, ConnectionResetError) as exc:
        LOG.info('Socket lost for %s: %r', peer_id, exc)
    finally:
        await unregister(peer_id, ws)
    return ws

async def index(request):
    if not GAME_HTML.is_file():
        return web.Response(text='index.html is missing on the server', status=500)
    data = GAME_HTML.read_text(encoding='utf-8')
    data = data.replace('window.__HEX_RELAY_ENABLED__=false;',
                        'window.__HEX_RELAY_ENABLED__=true;', 1)
    return web.Response(text=data, content_type='text/html', headers={'Cache-Control':'no-store'})

async def health(request):
    return web.json_response({'status':'ok', 'service':'Hex World Global Relay', 'online':len(clients)})

app=web.Application(client_max_size=MAX_MESSAGE_BYTES)
app.router.add_get('/', index)
app.router.add_get('/index.html', index)
app.router.add_get('/ws', ws_handler)
app.router.add_get('/health', health)

if __name__=='__main__':
    LOG.info('Serving Hex World at http://0.0.0.0:%s', PORT)
    web.run_app(app, host='0.0.0.0', port=PORT, access_log=None)
