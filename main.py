import asyncio
import hashlib
import json
import logging
# deprecated in v14, but legacy is simpler + removed in 4 years so i have time
from websockets.legacy.server import WebSocketServerProtocol, serve
from dataclasses import dataclass, field


@dataclass
class Bot:
    associated_ip: tuple[str, int]
    accessible_events: set[str] = field(default_factory=set)
    event_counts: dict[str, int] = field(default_factory=dict)
    channel_ids: set[int] = field(default_factory=set)


def compute_event_id(event: str, message_id: int, channel_id: int) -> int:
    hasher = hashlib.blake2b(digest_size=8)

    hasher.update(event.encode("utf-8"))
    hasher.update(message_id.to_bytes(8, "little", signed=False))
    hasher.update(channel_id.to_bytes(8, "little", signed=False))

    return int.from_bytes(hasher.digest(), "little", signed=False)


async def handle_connection(
    websocket: WebSocketServerProtocol,
    bot_map: dict[str, Bot],
    bot_map_lock: asyncio.Lock,
    handled_events: set[int],
    handled_events_lock: asyncio.Lock,
) -> None:
    peer_addr = websocket.remote_address
    if peer_addr is None:
        peer_addr = ("unknown", 0)

    logging.info("New connection")

    try:
        async for message in websocket:
            if not isinstance(message, str):
                continue

            texts = message.split(";")

            event = ""
            message_id = 1010
            channel_id = 1010
            bot_name = ""

            for arg in texts:
                parts = arg.split("=")

                if len(parts) != 2:
                    try:
                        await websocket.send(
                            f"Invalid body; expected 2 parts for arg {arg}"
                        )
                    except Exception:
                        logging.error("Failed to send invalid request body")
                        try:
                            await websocket.close()
                        except Exception:
                            pass
                    return

                key, value = parts

                if key == "ref":
                    try:
                        message_id = int(value)
                    except ValueError:
                        message_id = 1010
                elif key == "event":
                    event = value
                elif key == "name":
                    bot_name = value
                elif key == "channel":
                    try:
                        channel_id = int(value)
                    except ValueError:
                        channel_id = 1010
                else:
                    logging.warning("Invalid key %s!", key)

            logging.info("%s: %s", bot_name, event)

            async with bot_map_lock:
                if event == "get":
                    text_out = ""
                    for name, bot in bot_map.items():
                        counts_json = json.dumps(bot.event_counts)
                        text_out += f"{name} -> {counts_json}\n"

                    try:
                        await websocket.send(text_out)
                    except Exception:
                        pass
                    continue

                available_bots = sum(
                    1
                    for name, bot in bot_map.items()
                    if channel_id in bot.channel_ids
                    and event in bot.accessible_events
                    and name != bot_name
                )

                bot = bot_map.get(bot_name)
                if bot is None:
                    bot = Bot(associated_ip=peer_addr)
                    bot_map[bot_name] = bot

                bot.channel_ids.add(channel_id)
                bot.accessible_events.add(event)

                logging.debug("%s: %r", bot_name, bot)

                event_id = compute_event_id(event, message_id, channel_id)
                logging.debug("Event id: %s", event_id)

                event_count = bot.event_counts.get(event)
                if event_count is None:
                    event_count = 1
                    bot.event_counts[event] = event_count

                logging.info("%s, %s", event_count, available_bots)

                if available_bots >= 1 and event_count > 3:
                    logging.info("Bot has gotten too many events..")
                    try:
                        await websocket.send("handled")
                    except Exception:
                        pass
                    bot.event_counts[event] = event_count - 1
                    continue

                async with handled_events_lock:
                    if event_id in handled_events:
                        logging.info("Event already handled.. Skipping")
                        try:
                            await websocket.send("handled")
                        except Exception:
                            pass
                        continue

                    logging.info("Letting bot proceed")
                    bot.event_counts[event] = event_count + 1
                    try:
                        await websocket.send("unhandled")
                    except Exception:
                        pass
                    handled_events.add(event_id)

            print("")

    except Exception as exc:
        logging.error("Error decoding message %s", exc)

    finally:
        async with bot_map_lock:
            bot_name = None
            for name, bot in bot_map.items():
                if bot.associated_ip == peer_addr:
                    bot_name = name
                    break

            if bot_name is not None:
                logging.info("Dropped bot %s", bot_name)
                bot_map.pop(bot_name, None)


async def main() -> None:
    logging.basicConfig(level=logging.DEBUG)

    address = "0.0.0.0"
    port = 3960

    bot_map: dict[str, Bot] = {}
    handled_events: set[int] = set()

    bot_map_lock = asyncio.Lock()
    handled_events_lock = asyncio.Lock()

    async def handler(websocket: WebSocketServerProtocol) -> None:
        await handle_connection(
            websocket,
            bot_map,
            bot_map_lock,
            handled_events,
            handled_events_lock,
        )

    async with serve(handler, address, port):
        logging.info("Listening on %s:%s", address, port)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
