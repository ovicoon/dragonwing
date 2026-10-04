import { DurableObject } from "cloudflare:workers";

export interface Env {
  CHAT_ROOM: DurableObjectNamespace<ChatRoom>;
}

type Session = {
  id: string;
};

export class ChatRoom extends DurableObject<Env> {
  private sessions = new Map<string, WebSocket>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Hibernation에서 깨어났을 때
    // 기존 WebSocket 연결들을 다시 sessions에 등록
    for (const ws of this.ctx.getWebSockets()) {
      const session = ws.deserializeAttachment() as Session | null;

      if (session) {
        this.sessions.set(session.id, ws);
      }
    }

    // ping 요청은 DO를 깨우지 않고 자동으로 pong 응답
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair("ping", "pong"),
    );
  }

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", {
        status: 426,
      });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    this.ctx.acceptWebSocket(server);

    const id = crypto.randomUUID();

    // Hibernation 이후에도 이 연결의 ID를 복원할 수 있도록 저장
    server.serializeAttachment({
      id,
    });

    this.sessions.set(id, server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }
	private saveState(value: string) {
	this.ctx.storage.sql.exec(
		`CREATE TABLE IF NOT EXISTS server_state (
		id INTEGER PRIMARY KEY,
		value TEXT NOT NULL
		)`,
	);

	this.ctx.storage.sql.exec(
		`INSERT OR REPLACE INTO server_state (id, value)
		VALUES (1, ?)`,
		value,
	);
	}

	private loadState(): string | null {
	this.ctx.storage.sql.exec(
		`CREATE TABLE IF NOT EXISTS server_state (
		id INTEGER PRIMARY KEY,
		value TEXT NOT NULL
		)`,
	);

	const result = this.ctx.storage.sql.exec(
		`SELECT value FROM server_state WHERE id = 1`,
	);

	const row = result.one() as { value: string } | null;

	return row?.value ?? null;
	}

  webSocketMessage(
    ws: WebSocket,
    message: string | ArrayBuffer,
  ) {
    const session = ws.deserializeAttachment() as Session | null;

    console.log("message from:", session?.id);

    // 모든 연결에 broadcast
    for (const client of this.sessions.values()) {
      client.send(message);
    }
  }

  webSocketClose(ws: WebSocket) {
    const session = ws.deserializeAttachment() as Session | null;

    if (session) {
      this.sessions.delete(session.id);
    }
  }

  webSocketError(ws: WebSocket) {
    const session = ws.deserializeAttachment() as Session | null;

    if (session) {
      this.sessions.delete(session.id);
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.CHAT_ROOM.idFromName("main");
    const room = env.CHAT_ROOM.get(id);

    return room.fetch(request);
  },
};