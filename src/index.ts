import { DurableObject } from "cloudflare:workers";

export interface Env {
  MAIN: DurableObjectNamespace<MainDurableObject>;
}

type ClientInfo = {
  id: string;
};

type ClientMessage = {
  type: "data";
  payload: unknown;
};

type ServerMessage = {
  type: "data";
  clientId: string;
  payload: unknown;
  timestamp: number;
};

export class MainDurableObject extends DurableObject<Env> {
  /**
   * 현재 연결된 WebSocket
   *
   * 이 Map은 메모리 상태입니다.
   * Hibernation 후에는 constructor에서 복구합니다.
   */
  private clients = new Map<string, WebSocket>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    /**
     * Hibernation에서 깨어난 경우
     * 기존 WebSocket들을 다시 등록합니다.
     */
    for (const ws of this.ctx.getWebSockets()) {
      const info = ws.deserializeAttachment() as ClientInfo | null;

      if (info) {
        this.clients.set(info.id, ws);
      }
    }

    /**
     * ping 요청은 Durable Object를 깨우지 않고
     * 자동으로 pong을 반환합니다.
     */
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair("ping", "pong"),
    );

    /**
     * SQLite 테이블 초기화
     */
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        timestamp INTEGER NOT NULL
      )
    `);
  }

  async fetch(request: Request): Promise<Response> {
    /**
     * WebSocket 연결만 허용
     */
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket required", {
        status: 426,
      });
    }

    const pair = new WebSocketPair();

    const [client, server] = Object.values(pair);

    /**
     * Hibernation 가능한 WebSocket 연결
     */
    this.ctx.acceptWebSocket(server);

    /**
     * 클라이언트 ID 생성
     */
    const clientId = crypto.randomUUID();

    /**
     * WebSocket에 clientId 저장
     *
     * Hibernation 후에도 복구할 수 있습니다.
     */
    server.serializeAttachment({
      id: clientId,
    });

    this.clients.set(clientId, server);

    /**
     * 접속한 클라이언트에게 자신의 ID 전달
     */
    server.send(
      JSON.stringify({
        type: "connected",
        clientId,
      }),
    );

    /**
     * 현재 연결된 클라이언트 수 전달
     */
    this.broadcast(
      {
        type: "clients",
        count: this.clients.size,
      },
      server,
    );

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  /**
   * 클라이언트 → 서버 메시지
   */
  webSocketMessage(
    ws: WebSocket,
    message: string | ArrayBuffer,
  ) {
    const info = ws.deserializeAttachment() as ClientInfo | null;

    if (!info) {
      return;
    }

    const clientId = info.id;

    let data: ClientMessage;

    try {
      const text =
        typeof message === "string"
          ? message
          : new TextDecoder().decode(message);

      data = JSON.parse(text);
    } catch {
      ws.send(
        JSON.stringify({
          type: "error",
          message: "Invalid JSON",
        }),
      );

      return;
    }

    /**
     * 데이터 메시지
     */
    if (data.type === "data") {
      this.handleData(clientId, data.payload);
      return;
    }

    /**
     * 알 수 없는 메시지
     */
    ws.send(
      JSON.stringify({
        type: "error",
        message: "Unknown message type",
      }),
    );
  }

  /**
   * 실제 데이터 처리
   */
  private handleData(
    clientId: string,
    payload: unknown,
  ) {
    const timestamp = Date.now();

    /**
     * JSON 문자열로 변환해서 SQLite에 저장
     */
    const payloadJson = JSON.stringify(payload);

    this.ctx.storage.sql.exec(
      `
      INSERT INTO events (
        client_id,
        payload,
        timestamp
      )
      VALUES (?, ?, ?)
      `,
      clientId,
      payloadJson,
      timestamp,
    );

    /**
     * 다른 클라이언트들에게 실시간 전달
     */
    const message: ServerMessage = {
      type: "data",
      clientId,
      payload,
      timestamp,
    };

    this.broadcast(message);
  }

  /**
   * 모든 클라이언트에게 broadcast
   */
  private broadcast(
    data: unknown,
    except?: WebSocket,
  ) {
    const message = JSON.stringify(data);

    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) {
        continue;
      }

      try {
        ws.send(message);
      } catch {
        // 연결이 끊어진 경우 무시
      }
    }
  }

  /**
   * WebSocket 종료
   */
  webSocketClose(ws: WebSocket) {
    this.removeClient(ws);
  }

  /**
   * WebSocket 오류
   */
  webSocketError(ws: WebSocket) {
    this.removeClient(ws);
  }

  /**
   * 연결 제거
   */
  private removeClient(ws: WebSocket) {
    const info = ws.deserializeAttachment() as ClientInfo | null;

    if (!info) {
      return;
    }

    this.clients.delete(info.id);

    this.broadcast({
      type: "clients",
      count: this.clients.size,
    });
  }
}

/**
 * Worker
 */
export default {
  async fetch(
    request: Request,
    env: Env,
  ): Promise<Response> {
    /**
     * 항상 하나의 Main Durable Object 사용
     */
    const id = env.MAIN.idFromName("main");

    const main = env.MAIN.get(id);

    return main.fetch(request);
  },
};