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
   * 메모리 상태이므로 Hibernation 후 constructor에서 복구합니다.
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
     * 이 Durable Object가 사용하는 SQLite 테이블입니다.
     *
     * 각 Durable Object는 자신의 독립적인 SQLite storage를
     * 가지고 있으므로 서비스별로 데이터가 분리됩니다.
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

  /**
   * WebSocket 연결
   */
  async fetch(request: Request): Promise<Response> {
    /**
     * WebSocket 연결만 허용합니다.
     */
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket required", {
        status: 426,
      });
    }

    const pair = new WebSocketPair();

    const [client, server] = Object.values(pair);

    /**
     * Hibernation 가능한 WebSocket으로 등록합니다.
     */
    this.ctx.acceptWebSocket(server);

    /**
     * 새로운 클라이언트 ID 생성
     */
    const clientId = crypto.randomUUID();

    /**
     * Hibernation 후에도 clientId를 복구할 수 있도록
     * WebSocket attachment에 저장합니다.
     */
    server.serializeAttachment({
      id: clientId,
    });

    /**
     * 메모리에도 등록
     */
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
     * 다른 클라이언트들에게 현재 접속자 수 전달
     *
     * 새로 접속한 본인은 제외합니다.
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
     * payload를 JSON 문자열로 변환해서 SQLite에 저장
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
     * 다른 클라이언트들에게 전달할 메시지
     */
    const message: ServerMessage = {
      type: "data",
      clientId,
      payload,
      timestamp,
    };

    /**
     * 현재 코드에서는 sender 자신도 받습니다.
     *
     * sender를 제외하고 싶다면:
     *
     * this.broadcast(
     *   message,
     *   this.clients.get(clientId),
     * );
     */
    this.broadcast(message);
  }

  /**
   * 모든 WebSocket에게 메시지 전달
   *
   * except가 있으면 해당 WebSocket은 제외합니다.
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
        /**
         * 연결이 끊어진 경우 무시합니다.
         */
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

    /**
     * 남아 있는 클라이언트들에게 현재 접속자 수 전달
     */
    this.broadcast({
      type: "clients",
      count: this.clients.size,
    });
  }
}

/**
 * Worker
 *
 * URL 구조:
 *
 *   /ws/game-1
 *   /ws/iot-1
 *   /ws/game-2
 *
 * 각각 서로 다른 Durable Object를 사용합니다.
 */
export default {
  async fetch(
    request: Request,
    env: Env,
  ): Promise<Response> {
    const url = new URL(request.url);

    /**
     * URL의 첫 번째 path를 확인합니다.
     *
     * 예:
     *
     * /ws/game-1
     *
     * path:
     * ["ws", "game-1"]
     */
    const parts = url.pathname
      .split("/")
      .filter(Boolean);

    /**
     * /ws/{serviceName} 형식만 허용
     */
    if (
      parts.length !== 2 ||
      parts[0] !== "ws"
    ) {
      return new Response(
        "Use /ws/{serviceName}",
        {
          status: 400,
        },
      );
    }

    const serviceName = parts[1];

    /**
     * 서비스 이름을 Durable Object 이름으로 사용합니다.
     *
     * 예:
     *
     * game-1 → DO(game-1)
     * iot-1  → DO(iot-1)
     * game-2 → DO(game-2)
     */
    const id = env.MAIN.idFromName(serviceName);

    /**
     * 해당 Durable Object 인스턴스를 가져옵니다.
     */
    const durableObject = env.MAIN.get(id);

    /**
     * 요청을 Durable Object에게 전달합니다.
     */
    return durableObject.fetch(request);
  },
};
