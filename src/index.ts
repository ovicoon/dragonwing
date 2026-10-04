import { DurableObject } from "cloudflare:workers";

/* =========================================================
 * Environment
 * ========================================================= */

export interface Env {
  GAME: DurableObjectNamespace<DragonWingGame>;
  IOT: DurableObjectNamespace<DragonWingIoT>;
}

/* =========================================================
 * Common Constants
 * ========================================================= */

const MAX_MESSAGE_BYTES = 16 * 1024;
const MAX_STATE_BYTES = 256 * 1024;

const MAX_CHAT_LENGTH = 2000;
const MAX_TARGET_ID_LENGTH = 128;
const MAX_DEVICE_ID_LENGTH = 128;

/* =========================================================
 * Common Types
 * ========================================================= */

type ClientInfo = {
  clientId: string;
};

/*
 * 서비스별 메시지를 저장하는 공통 상태 구조.
 *
 * Game이든 IoT든 구조는 동일하다.
 * payload 타입만 서비스마다 달라진다.
 */
type RealtimeState<M> = {
  lastEvent: {
    clientId: string;
    payload: M;
    timestamp: number;
  } | null;
};

/*
 * SQLite에서 읽은 row.
 */
type StateRow = {
  payload: string;
  updated_at: number;
};

/* =========================================================
 * Game Message
 * ========================================================= */

/*
 * sender의 clientId는 메시지에 넣지 않는다.
 *
 * 서버가 WebSocket attachment에서 가져온다.
 */
export type GameMessage =
  | {
      type: "move";
      x: number;
      y: number;
    }
  | {
      type: "attack";
      targetId: string;
    }
  | {
      type: "chat";
      text: string;
    };

/* =========================================================
 * IoT Message
 * ========================================================= */

export type IoTMessage =
  | {
      type: "temperature";
      deviceId: string;
      value: number;
    }
  | {
      type: "humidity";
      deviceId: string;
      value: number;
    }
  | {
      type: "switch";
      deviceId: string;
      value: boolean;
    };

/* =========================================================
 * Common Validation Utilities
 * ========================================================= */

function isObject(
  value: unknown,
): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null
  );
}

function isFiniteNumber(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isFinite(value)
  );
}

function isBoundedString(
  value: unknown,
  maxLength: number,
): value is string {
  return (
    typeof value === "string" &&
    value.length <= maxLength
  );
}

/* =========================================================
 * Game Message Validation
 * ========================================================= */

function isGameMessage(
  value: unknown,
): value is GameMessage {
  if (!isObject(value)) {
    return false;
  }

  switch (value.type) {
    case "move":
      return (
        isFiniteNumber(value.x) &&
        isFiniteNumber(value.y)
      );

    case "attack":
      return isBoundedString(
        value.targetId,
        MAX_TARGET_ID_LENGTH,
      );

    case "chat":
      return isBoundedString(
        value.text,
        MAX_CHAT_LENGTH,
      );

    default:
      return false;
  }
}

/* =========================================================
 * IoT Message Validation
 * ========================================================= */

function isIoTMessage(
  value: unknown,
): value is IoTMessage {
  if (!isObject(value)) {
    return false;
  }

  switch (value.type) {
    case "temperature":
      return (
        isBoundedString(
          value.deviceId,
          MAX_DEVICE_ID_LENGTH,
        ) &&
        isFiniteNumber(value.value)
      );

    case "humidity":
      return (
        isBoundedString(
          value.deviceId,
          MAX_DEVICE_ID_LENGTH,
        ) &&
        isFiniteNumber(value.value)
      );

    case "switch":
      return (
        isBoundedString(
          value.deviceId,
          MAX_DEVICE_ID_LENGTH,
        ) &&
        typeof value.value === "boolean"
      );

    default:
      return false;
  }
}

/* =========================================================
 * WebSocket Message Parser
 * ========================================================= */

function parseWebSocketMessage(
  message: string | ArrayBuffer,
): unknown | null {
  /*
   * ArrayBuffer는 decode하기 전에 크기를 검사한다.
   */
  if (message instanceof ArrayBuffer) {
    if (
      message.byteLength >
      MAX_MESSAGE_BYTES
    ) {
      return null;
    }
  }

  const text =
    typeof message === "string"
      ? message
      : new TextDecoder().decode(message);

  /*
   * 문자열은 UTF-8 byte 기준으로 제한한다.
   */
  const byteLength =
    new TextEncoder().encode(text).byteLength;

  if (
    byteLength >
    MAX_MESSAGE_BYTES
  ) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

/* =========================================================
 * Generic Realtime Durable Object
 *
 * 모든 서비스의 공통 동작.
 *
 * Game / IoT / 앞으로 추가될 다른 서비스도
 * 이 클래스를 상속한다.
 *
 * 차이는:
 *   - serviceName
 *   - message type
 *   - message validator
 *
 * 뿐이다.
 * ========================================================= */

abstract class RealtimeDurableObject<
  M,
> extends DurableObject<Env> {

  /*
   * 현재 연결된 WebSocket 목록.
   *
   * 이것은 영속 데이터가 아니다.
   *
   * Hibernation 이후 constructor가 실행되면
   * ctx.getWebSockets() + attachment로 다시 구성한다.
   */
  private clients =
    new Map<string, WebSocket>();

  /*
   * 현재 서비스 상태.
   *
   * 영속 데이터는 SQLite이고,
   * 이 값은 메모리 캐시다.
   */
  private state:
    RealtimeState<M>;

  /*
   * SQLite의 마지막 갱신 시간.
   */
  private updatedAt =
    0;

  /*
   * 서비스가 구현해야 하는 부분.
   */
  protected abstract getServiceName(): string;

  protected abstract validateMessage(
    value: unknown,
  ): value is M;

  constructor(
    ctx: DurableObjectState,
    env: Env,
  ) {
    super(ctx, env);

    /* =====================================================
     * 공통 상태 테이블
     *
     * DO는 서비스별로 분리되어 있으므로
     * 모든 DO에서 같은 테이블 이름을 사용해도 된다.
     *
     * 각 DO에는 이 row 하나만 존재한다.
     * ===================================================== */

    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS realtime_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        payload TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);

    /* =====================================================
     * Application State Recovery
     *
     * DO instance가 새로 생성되어도
     * SQLite에 저장된 상태를 복구한다.
     * ===================================================== */

    const row =
      this.ctx.storage.sql
        .exec(
          `
            SELECT
              payload,
              updated_at
            FROM realtime_state
            WHERE id = 1
          `,
        )
        .toArray()[0] as
          | StateRow
          | undefined;

    if (row !== undefined) {
      try {
        const parsed =
          JSON.parse(row.payload) as
            RealtimeState<M>;

        if (
          isObject(parsed) &&
          (
            parsed.lastEvent === null ||
            isObject(parsed.lastEvent)
          )
        ) {
          this.state = parsed;
          this.updatedAt =
            Number(row.updated_at);
        } else {
          this.state =
            this.createInitialState();
        }
      } catch {
        /*
         * 저장 데이터가 잘못되었으면
         * 빈 상태에서 시작한다.
         */
        this.state =
          this.createInitialState();

        this.updatedAt =
          0;
      }
    } else {
      this.state =
        this.createInitialState();

      this.updatedAt =
        0;
    }

    /* =====================================================
     * WebSocket Recovery
     *
     * Hibernation 이후 살아 있는 WebSocket을 복구한다.
     * ===================================================== */

    for (
      const ws of this.ctx.getWebSockets()
    ) {
      const info =
        ws.deserializeAttachment() as
          | ClientInfo
          | null;

      if (
        info === null ||
        typeof info.clientId !== "string"
      ) {
        continue;
      }

      this.clients.set(
        info.clientId,
        ws,
      );
    }

    /* =====================================================
     * Automatic Ping / Pong
     * ===================================================== */

    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(
        "ping",
        "pong",
      ),
    );
  }

  /* =======================================================
   * Initial State
   * ======================================================= */

  private createInitialState():
    RealtimeState<M> {
    return {
      lastEvent: null,
    };
  }

  /* =======================================================
   * Connection
   * ======================================================= */

  async fetch(
    request: Request,
  ): Promise<Response> {

    if (
      request.headers
        .get("Upgrade")
        ?.toLowerCase() !== "websocket"
    ) {
      return new Response(
        "WebSocket required",
        {
          status: 426,
        },
      );
    }

    const pair =
      new WebSocketPair();

    const client = pair[0];
    const server = pair[1];

    /* =====================================================
     * Register WebSocket for Hibernation
     * ===================================================== */

    this.ctx.acceptWebSocket(
      server,
      [this.getServiceName()],
    );

    /* =====================================================
     * Anonymous Connection ID
     * ===================================================== */

    const clientId =
      crypto.randomUUID();

    const clientInfo: ClientInfo = {
      clientId,
    };

    /*
     * Hibernation 복구용 attachment.
     */
    server.serializeAttachment(
      clientInfo,
    );

    this.clients.set(
      clientId,
      server,
    );

    /* =====================================================
     * Connected Event
     * ===================================================== */

    this.send(
      server,
      {
        type: "connected",
        service: this.getServiceName(),
        clientId,
      },
    );

    /* =====================================================
     * Client Count
     * ===================================================== */

    this.broadcast({
      type: "clients",
      service: this.getServiceName(),
      count: this.clients.size,
    });

    /* =====================================================
     * Current State
     *
     * 새로 접속한 클라이언트는
     * 현재 서비스 상태를 바로 받는다.
     * ===================================================== */

    this.send(
      server,
      {
        type: "state",
        service: this.getServiceName(),
        state: this.state,
        updatedAt: this.updatedAt,
      },
    );

    return new Response(
      null,
      {
        status: 101,
        webSocket: client,
      },
    );
  }

  /* =======================================================
   * WebSocket Message
   * ======================================================= */

  webSocketMessage(
    ws: WebSocket,
    message: string | ArrayBuffer,
  ): void {

    /* =====================================================
     * Sender Identification
     *
     * 클라이언트가 보내는 ID를 사용하지 않는다.
     * 서버가 만든 attachment의 clientId를 사용한다.
     * ===================================================== */

    const info =
      ws.deserializeAttachment() as
        | ClientInfo
        | null;

    if (
      info === null ||
      typeof info.clientId !== "string"
    ) {
      this.sendError(
        ws,
        "Client information not found",
      );

      return;
    }

    /* =====================================================
     * Parse
     * ===================================================== */

    const parsed =
      parseWebSocketMessage(message);

    if (parsed === null) {
      this.sendError(
        ws,
        "Invalid JSON or message too large",
      );

      return;
    }

    /* =====================================================
     * Validation
     * ===================================================== */

    if (
      !this.validateMessage(parsed)
    ) {
      this.sendError(
        ws,
        "Invalid message",
      );

      return;
    }

    /* =====================================================
     * Process
     * ===================================================== */

    this.handleMessage(
      ws,
      info.clientId,
      parsed,
    );
  }

  /* =======================================================
   * Common Message Handling
   * ======================================================= */

  private handleMessage(
    ws: WebSocket,
    clientId: string,
    message: M,
  ): void {

    const timestamp =
      Date.now();

    const nextState:
      RealtimeState<M> = {
        lastEvent: {
          clientId,
          payload: message,
          timestamp,
        },
      };

    /* =====================================================
     * Serialize State
     * ===================================================== */

    let serializedState: string;

    try {
      serializedState =
        JSON.stringify(nextState);
    } catch {
      this.sendError(
        ws,
        "Failed to serialize state",
      );

      return;
    }

    /* =====================================================
     * State Size Limit
     * ===================================================== */

    const stateBytes =
      new TextEncoder()
        .encode(serializedState)
        .byteLength;

    if (
      stateBytes >
      MAX_STATE_BYTES
    ) {
      this.sendError(
        ws,
        "State is too large",
      );

      return;
    }

    /* =====================================================
     * Persistent State Update
     *
     * 항상 id = 1만 사용한다.
     *
     * 따라서 이벤트가 아무리 많이 들어와도
     * SQLite row가 계속 증가하지 않는다.
     * ===================================================== */

    try {
      this.ctx.storage.sql.exec(
        `
          INSERT INTO realtime_state (
            id,
            payload,
            updated_at
          )
          VALUES (1, ?, ?)

          ON CONFLICT(id)
          DO UPDATE SET
            payload = excluded.payload,
            updated_at = excluded.updated_at
        `,
        serializedState,
        timestamp,
      );
    } catch {
      this.sendError(
        ws,
        "Failed to persist state",
      );

      return;
    }

    /* =====================================================
     * Memory State Update
     *
     * SQLite 저장 성공 후에 갱신한다.
     * ===================================================== */

    this.state =
      nextState;

    this.updatedAt =
      timestamp;

    /* =====================================================
     * Broadcast
     *
     * 기존 코드와 같은 개념:
     *
     * event =
     *   sender + payload + timestamp
     *
     * 현재 state 자체는 새 접속 시
     * state 메시지로 전달된다.
     * ===================================================== */

    this.broadcast({
      type: "event",
      service: this.getServiceName(),
      clientId,
      payload: message,
      timestamp,
    });
  }

  /* =======================================================
   * Broadcast
   * ======================================================= */

  protected broadcast(
    data: unknown,
    except?: WebSocket,
  ): void {

    let serialized: string;

    try {
      serialized =
        JSON.stringify(data);
    } catch {
      return;
    }

    const tag =
      this.getServiceName();

    const sockets =
      this.ctx.getWebSockets(tag);

    for (const ws of sockets) {
      if (ws === except) {
        continue;
      }

      try {
        ws.send(serialized);
      } catch {
        /*
         * 실제 close/error는
         * Cloudflare WebSocket lifecycle에서 처리한다.
         */
      }
    }
  }

  /* =======================================================
   * Send
   * ======================================================= */

  private send(
    ws: WebSocket,
    data: unknown,
  ): void {

    try {
      ws.send(
        JSON.stringify(data),
      );
    } catch {
      // ignore
    }
  }

  /* =======================================================
   * Error
   * ======================================================= */

  protected sendError(
    ws: WebSocket,
    message: string,
  ): void {

    this.send(
      ws,
      {
        type: "error",
        service: this.getServiceName(),
        message,
      },
    );
  }

  /* =======================================================
   * WebSocket Close
   * ======================================================= */

  webSocketClose(
    ws: WebSocket,
  ): void {
    this.removeClient(ws);
  }

  /* =======================================================
   * WebSocket Error
   * ======================================================= */

  webSocketError(
    ws: WebSocket,
  ): void {
    this.removeClient(ws);
  }

  /* =======================================================
   * Remove Client
   * ======================================================= */

  private removeClient(
    ws: WebSocket,
  ): void {

    const info =
      ws.deserializeAttachment() as
        | ClientInfo
        | null;

    if (info === null) {
      return;
    }

    this.clients.delete(
      info.clientId,
    );

    this.broadcast({
      type: "clients",
      service: this.getServiceName(),
      count: this.clients.size,
    });
  }
}

/* =========================================================
 * Game Durable Object
 *
 * 동작은 부모 클래스가 모두 처리한다.
 * Game은 데이터 형식만 정의한다.
 * ========================================================= */

export class DragonWingGame
  extends RealtimeDurableObject<GameMessage> {

  protected getServiceName(): string {
    return "game";
  }

  protected validateMessage(
    value: unknown,
  ): value is GameMessage {
    return isGameMessage(value);
  }
}

/* =========================================================
 * IoT Durable Object
 *
 * Game과 동작 방식은 완전히 동일하다.
 * 데이터 형식만 IoT용으로 다르다.
 * ========================================================= */

export class DragonWingIoT
  extends RealtimeDurableObject<IoTMessage> {

  protected getServiceName(): string {
    return "iot";
  }

  protected validateMessage(
    value: unknown,
  ): value is IoTMessage {
    return isIoTMessage(value);
  }
}

/* =========================================================
 * Worker Entry Point
 * ========================================================= */

export default {
  async fetch(
    request: Request,
    env: Env,
  ): Promise<Response> {

    const url =
      new URL(request.url);

    /* =====================================================
     * Game Service
     * ===================================================== */

    if (
      url.pathname === "/ws/game"
    ) {
      const id =
        env.GAME.idFromName("main");

      const game =
        env.GAME.get(id);

      return game.fetch(request);
    }

    /* =====================================================
     * IoT Service
     * ===================================================== */

    if (
      url.pathname === "/ws/iot"
    ) {
      const id =
        env.IOT.idFromName("main");

      const iot =
        env.IOT.get(id);

      return iot.fetch(request);
    }

    /* =====================================================
     * Unsupported Route
     * ===================================================== */

    return new Response(
      "Use /ws/game or /ws/iot",
      {
        status: 404,
      },
    );
  },
};