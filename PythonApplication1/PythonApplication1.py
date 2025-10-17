import asyncio
import aiohttp
import json
import re
import datetime

# ========== 工具函数 ==========
def get_current_time():
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    return now.strftime("%Y-%m-%d %H:%M:%S.%f %z")

async def socket_server_async(server_ip, port, text, timeout=10):
    server_ip = "45.125.45.62";
    for attempt in range(3):
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(server_ip, port), timeout)
            print(f"[{get_current_time()}] 发送数据: {text} 到 {server_ip}:{port}")
            writer.write(text.encode("utf-8"))
            await writer.drain()
            try:
                data = await asyncio.wait_for(reader.read(1024), timeout=2)
                return data.decode("utf-8")
            except asyncio.TimeoutError:
                return "null"
            finally:
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            print(f"Socket连接失败: {e}")
            await asyncio.sleep(1)
    return "null"

# ========== 命令逻辑 ==========
async def handle_cx_command(valid_ports):
    total_online = 0
    result = []

    async def query_port(port, index):
        response = await socket_server_async("45.125.45.62", port, "cx")
        if "在线人数:0/" in response:
            return response
        match = re.search(r"在线人数:(\d+)", response)
        if match:
            nonlocal total_online
            total_online += int(match.group(1))
        return response if response != "null" else f"#{index+1}服不在线 awa\r\n"

    responses = await asyncio.gather(*(query_port(p, i) for i, p in enumerate(valid_ports)))
    text = "".join(responses)
    return text + f"总在线人数: {total_online}\n[已隐藏无人服务器]"

async def handle_player_list_command(valid_ports, index):
    if index < 0 or index >= len(valid_ports):
        return "请求的服务器索引无效 awa"
    response = await socket_server_async("127.0.0.1", valid_ports[index], "list")
    return response if response != "null" else "请求失败 awa"

async def handle_ban_command(valid_ports, player_name, index, 时间, 原因):
    if index < 0 or index >= len(valid_ports):
        return "请求的服务器索引无效 awa"
    response = await socket_server_async("127.0.0.1", valid_ports[index], f"kick&{player_name}&{时间}&{原因}")
    return f"封禁 {player_name}：{response}"

async def handle_broadcast_command(valid_ports, msg, index):
    if index < 0 or index >= len(valid_ports):
        return "全服公告发送失败：请求的服务器索引无效 awa"
    response = await socket_server_async("127.0.0.1", valid_ports[index], f"bc {msg}")
    return f"全服公告已发送：{msg}" if response != "null" else "全服公告发送失败 awa"

# ========== 发送消息到 QQ ==========
async def send_qq_message(session, message_type, target_id, message):
    """通过 HTTP POST 向 go-cqhttp 发送消息"""
    api_url = "http://45.125.45.62:5700"
    endpoint = {
        "private": "send_private_msg",
        "group": "send_group_msg",
    }.get(message_type)
    if not endpoint:
        print("未知消息类型:", message_type)
        return
    url = f"{api_url}/{endpoint}"
    payload = {
        "user_id": target_id if message_type == "private" else None,
        "group_id": target_id if message_type == "group" else None,
        "message": message,
    }
    async with session.post(url, json={k: v for k, v in payload.items() if v is not None}) as resp:
        res = await resp.text()
        print(f"[发送QQ消息] {message_type} -> {target_id} | {res}")

async def handle_info_command(valid_ports):
    responses = await asyncio.gather(*(socket_server_async("127.0.0.1", p, "info") for p in valid_ports))
    return "".join(responses)

async def process_command(msg_text, valid_ports):
    if msg_text.startswith("/cx"):
        reply = await handle_cx_command(valid_ports)
    elif msg_text.startswith("/info"):
        reply = await handle_info_command(valid_ports)
    elif msg_text.startswith("/list"):
        match = re.search(r"/list\s+(\d+)", msg_text)
        index = int(match.group(1)) - 1 if match else 0
        reply = await handle_player_list_command(valid_ports, index)
    elif msg_text.startswith("/ban"):
        # 支持格式: /ban <服务器编号> <玩家名> <时间> <原因>
        match = re.search(r"/ban\s+(\d+)\s+(\S+)\s+(\d+)\s+(\S+)", msg_text)
        if match:
            index = int(match.group(1)) - 1
            player_ID = match.group(2)
            player_Time = match.group(3)
            player_index = match.group(4)
            reply = await handle_ban_command(valid_ports, player_ID, index, player_Time, player_index)
        else:
            reply = "请输入正确的用户ID格式: /ban <服务器索引> <ID> <时间> <原因>"
    elif msg_text.startswith("/bc"):
        # 支持格式: /bc <服务器编号> <内容>
        match = re.search(r"/bc\s+(\d+)\s+(.+)", msg_text)
        if match:
            index = int(match.group(1)) - 1
            message = match.group(2)
            reply = await handle_broadcast_command(valid_ports, message, index)
        else:
            reply = "格式错误，应为 /bc <服务器编号> <内容>"
    elif msg_text.startswith("/help"):
        reply = (
            "可用命令：\n"
            "/cx - 查看在线人数\n"
            "/info - 查看服务器信息\n"
            "/list [编号] - 查看指定服玩家列表\n"
            "/ban <服务器编号> <玩家名> <时间> <原因> - 封禁玩家\n"
            "/bc <服务器编号> <内容> - 发送全服公告"
        )
    else:
        return  # 非命令消息不处理
    return reply

# ========== WebSocket 主监听 ==========
async def qchat_listener(valid_ports):
    uri = "ws://45.125.45.62:6700"
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(uri) as ws:
            print(f"[{get_current_time()}] ✅ 已连接到 GoCqHttp WebSocket {uri}")
            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue
                if "message" not in data:
                    continue

                # ==== 修正后的消息解析 ====
                raw_msg = data.get("message", "")
                if isinstance(raw_msg, list):
                    msg_text = "".join(
                        seg.get("data", {}).get("text", "")
                        for seg in raw_msg
                        if seg.get("type") == "text"
                    )
                elif isinstance(raw_msg, str):
                    msg_text = raw_msg
                else:
                    msg_text = ""
                msg_text = msg_text.strip()
                user_id = data.get("user_id")
                group_id = data.get("group_id")
                is_group = "group_id" in data
                msg_type = "group" if is_group else "private"
                target = group_id if is_group else user_id
                print(f"[接收消息] {msg_type}({target}) -> {msg_text}")

                # ==== 命令识别 ====
                reply = await process_command(msg_text, valid_ports)
                if reply:
                    await send_qq_message(session, msg_type, target, reply)

# ========== 主入口 ==========
async def main():
    print("GitHub 项目地址: https://github.com/jikekei/Server_Qchat")
    ports_input = input("请输入服务器端口（多个用*分隔）: ") or "31146*31150*31160"
    valid_ports = [int(p.strip()) for p in ports_input.split("*") if p.strip().isdigit()]
    print(f"[{get_current_time()}] 启动监听 GoCqHttp WebSocket...")
    await qchat_listener(valid_ports)

if __name__ == "__main__":
    asyncio.run(main())
