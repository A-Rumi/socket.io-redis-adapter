import uid2 = require("uid2");
import msgpack = require("notepack.io");
import { Adapter, BroadcastOptions, Room, SocketId } from "socket.io-adapter";

const debug = require("debug")("socket.io-redis");

module.exports = exports = createAdapter;

/**
 * Request types, for messages between nodes
 */

enum RequestType {
  SOCKETS = 0,
  ALL_ROOMS = 1,
  REMOTE_JOIN = 2,
  REMOTE_LEAVE = 3,
  REMOTE_DISCONNECT = 4,
  REMOTE_FETCH = 5,
  SERVER_SIDE_EMIT = 6,
  BROADCAST,
  BROADCAST_CLIENT_COUNT,
  BROADCAST_ACK,
}

interface Request {
  type: RequestType;
  resolve: Function;
  timeout: NodeJS.Timeout;
  numSub?: number;
  msgCount?: number;
  [other: string]: any;
}

interface AckRequest {
  clientCountCallback: (clientCount: number) => void;
  ack: (...args: any[]) => void;
}

interface Parser {
  decode: (msg: any) => any;
  encode: (msg: any) => any;
}

const isNumeric = (str) => !isNaN(str) && !isNaN(parseFloat(str));

export interface RedisAdapterOptions {
  /**
   * the name of the key to pub/sub events on as prefix
   * @default socket.io
   */
  key: string;
  /**
   * after this timeout the adapter will stop waiting from responses to request
   * @default 5000
   */
  requestsTimeout: number;
  /**
   * Whether to publish a response to the channel specific to the requesting node.
   *
   * - if true, the response will be published to `${key}-request#${nsp}#${uid}#`
   * - if false, the response will be published to `${key}-request#${nsp}#`
   *
   * This option currently defaults to false for backward compatibility, but will be set to true in the next major
   * release.
   *
   * @default false
   */
  publishOnSpecificResponseChannel: boolean;
  /**
   * 레디스에 보내고 받을때 인코딩, 디코딩에 사용
   * The parser to use for encoding and decoding messages sent to Redis.
   * This option defaults to using `notepack.io`, a MessagePack implementation.
   */
  parser: Parser;
}

/**
 * Returns a function that will create a RedisAdapter instance.
 *
 * @param pubClient - a Redis client that will be used to publish messages
 * @param subClient - a Redis client that will be used to receive messages (put in subscribed state)
 * @param opts - additional options
 *
 * @public
 */
export function createAdapter(
  pubClient: any,
  subClient: any,
  opts?: Partial<RedisAdapterOptions>
) {
  return function (nsp) {
    return new RedisAdapter(nsp, pubClient, subClient, opts);
  };
}

export class RedisAdapter extends Adapter {
  public readonly uid;
  public readonly requestsTimeout: number;
  public readonly publishOnSpecificResponseChannel: boolean;
  public readonly parser: Parser;

  private readonly channel: string;
  private readonly requestChannel: string;
  private readonly responseChannel: string;
  private readonly specificResponseChannel: string;
  private requests: Map<string, Request> = new Map();
  private ackRequests: Map<string, AckRequest> = new Map();
  private redisListeners: Map<string, Function> = new Map();
  // private redisClientList = new Array();
  // private pubClient;
  // private subClient;

  /**
   * Adapter constructor.
   *
   * @param nsp - the namespace
   * @param pubClient - a Redis client that will be used to publish messages
   * @param subClient - a Redis client that will be used to receive messages (put in subscribed state)
   * @param opts - additional options
   *
   * @public
   */
  constructor(
    nsp: any,
    // readonly urlList: string[],
    readonly pubClient: any,
    readonly subClient: any,
    opts: Partial<RedisAdapterOptions> = {}
  ) {
    super(nsp);

    // this.redisClientList = urlList.map((url) =>
    //   createClient({ url: `redis://${url}` })
    // );
    // this.pubClient = this.redisClientList[0];
    // this.subClient = this.redisClientList[0];

    this.uid = uid2(6);
    this.requestsTimeout = opts.requestsTimeout || 5000;
    this.publishOnSpecificResponseChannel = !!opts.publishOnSpecificResponseChannel;
    this.parser = opts.parser || msgpack; // 따로 parser 지정 없으면 msgpack 사용

    const prefix = opts.key || "socket.io";

    this.channel = prefix + "#" + nsp.name + "#";
    this.requestChannel = prefix + "-request#" + this.nsp.name + "#";
    this.responseChannel = prefix + "-response#" + this.nsp.name + "#";
    this.specificResponseChannel = this.responseChannel + this.uid + "#";

    // 레디스 클라이언트인 경우
    const isRedisV4 = typeof this.pubClient.pSubscribe === "function";
    if (isRedisV4) {
      // Map에 psub 키값으로 저장
      this.redisListeners.set("psub", (msg, channel) => {
        this.onmessage(null, channel, msg);
      });

      // Map에 sub 키값으로 저장
      this.redisListeners.set("sub", (msg, channel) => {
        this.onrequest(channel, msg);
      });

      // 레디스 패턴채널 구독 (patterns, listener, bufferMode)
      this.subClient.pSubscribe(
        this.channel + "*",
        this.redisListeners.get("psub"),
        true
      );

      // 지정채널 메시지 구독 (patterns, listener, bufferMode)
      this.subClient.subscribe(
        [
          this.requestChannel,
          this.responseChannel,
          this.specificResponseChannel,
        ],
        this.redisListeners.get("sub"),
        true
      );
    } else {
      // Map에 pmessageBuffer, messageBuffer 키값으로 저장
      this.redisListeners.set("pmessageBuffer", this.onmessage.bind(this));
      this.redisListeners.set("messageBuffer", this.onrequest.bind(this));

      // 채널 패턴 구독
      this.subClient.psubscribe(this.channel + "*");

      // EventEmitter.on()
      // eventName, listener
      // listener : 이벤트 이름으로 된 배열 끝에 리스너추가 (갖고있는지 체크안함 그냥 추가)
      // pmessageBuffer 배열 끝에 리스너 추가
      this.subClient.on(
        "pmessageBuffer",
        this.redisListeners.get("pmessageBuffer")
      );

      // 지정채널 메시지 구독 (patterns)
      this.subClient.subscribe([
        this.requestChannel,
        this.responseChannel,
        this.specificResponseChannel,
      ]);
      // messageBuffer 배열 끝에 리스너 추가
      this.subClient.on(
        "messageBuffer",
        this.redisListeners.get("messageBuffer")
      );
    }

    // 에러 핸들링
    const registerFriendlyErrorHandler = (redisClient) => {
      redisClient.on("error", () => {
        if (redisClient.listenerCount("error") === 1) {
          console.warn("missing 'error' handler on this Redis client");
        }
      });
    };

    registerFriendlyErrorHandler(this.pubClient);
    registerFriendlyErrorHandler(this.subClient);
  }

  /**
   * Called with a subscription message
   * 구독 메시지 확인
   * @private
   */
  private onmessage(pattern, channel, msg) {
    channel = channel.toString();

    // 채널 매칭 확인
    const channelMatches = channel.startsWith(this.channel);
    if (!channelMatches) {
      return debug("ignore different channel");
    }

    // 방이 비었는지 확인
    const room = channel.slice(this.channel.length, -1);
    if (room !== "" && !this.hasRoom(room)) {
      return debug("ignore unknown room %s", room);
    }

    // 레디스 메시지 디코드
    const args = this.parser.decode(msg);

    const [uid, packet, opts] = args;
    if (this.uid === uid) return debug("ignore same uid");

    if (packet && packet.nsp === undefined) {
      packet.nsp = "/";
    }

    if (!packet || packet.nsp !== this.nsp.name) {
      return debug("ignore different namespace");
    }
    opts.rooms = new Set(opts.rooms);
    opts.except = new Set(opts.except);

    // socket.io-adapter의 broadcast 접근
    super.broadcast(packet, opts);
  }

  // 룸을 가졌는가에 대한 체크
  private hasRoom(room): boolean {
    // @ts-ignore
    const hasNumericRoom = isNumeric(room) && this.rooms.has(parseFloat(room));
    return hasNumericRoom || this.rooms.has(room);
  }

  /**
   * Called on request from another node
   * redis에 키 값 저장(set)할때 호출 함수 - 다른 노드 요청에 따라 호출
   * @private
   */
  private async onrequest(channel, msg) {
    channel = channel.toString();

    // 찾는 채널이 있다면 메시지 수신
    if (channel.startsWith(this.responseChannel)) {
      return this.onresponse(channel, msg);
    } else if (!channel.startsWith(this.requestChannel)) {
      return debug("ignore different channel");
    }

    let request;

    try {
      // if the buffer starts with a "{" character
      if (msg[0] === 0x7b) {
        request = JSON.parse(msg.toString());
      } else {
        request = this.parser.decode(msg);
      }
    } catch (err) {
      debug("ignoring malformed request");
      return;
    }

    debug("received request %j", request);

    let response, socket;

    switch (request.type) {
      case RequestType.SOCKETS:
        if (this.requests.has(request.requestId)) {
          return;
        }

        // 소켓아이디별 리스트 조회
        const sockets = await super.sockets(new Set(request.rooms));

        // 소켓 설정
        response = JSON.stringify({
          requestId: request.requestId,
          sockets: [...sockets],
        });

        // 다른 요청 노드로 res 전달
        this.publishResponse(request, response);
        break;

      case RequestType.ALL_ROOMS:
        if (this.requests.has(request.requestId)) {
          return;
        }

        // 룸 설정
        response = JSON.stringify({
          requestId: request.requestId,
          rooms: [...this.rooms.keys()],
        });

        this.publishResponse(request, response);
        break;

      case RequestType.REMOTE_JOIN:
        if (request.opts) {
          const opts = {
            rooms: new Set<Room>(request.opts.rooms),
            except: new Set<Room>(request.opts.except),
          };
          // 특정 룸 들어감
          return super.addSockets(opts, request.rooms);
        }

        // 소켓아이디로 소켓 찾기
        socket = this.nsp.sockets.get(request.sid);
        if (!socket) {
          return;
        }

        // 룸 들어감
        socket.join(request.room);

        response = JSON.stringify({
          requestId: request.requestId,
        });

        // 다른 요청 노드로 res 전달
        this.publishResponse(request, response);
        break;

      case RequestType.REMOTE_LEAVE:
        if (request.opts) {
          const opts = {
            rooms: new Set<Room>(request.opts.rooms),
            except: new Set<Room>(request.opts.except),
          };
          // 특정 룸 나감
          return super.delSockets(opts, request.rooms);
        }

        socket = this.nsp.sockets.get(request.sid);
        if (!socket) {
          return;
        }

        // 룸 나감
        socket.leave(request.room);

        response = JSON.stringify({
          requestId: request.requestId,
        });

        // 다른 요청 노드로 res 전달
        this.publishResponse(request, response);
        break;

      case RequestType.REMOTE_DISCONNECT:
        if (request.opts) {
          const opts = {
            rooms: new Set<Room>(request.opts.rooms),
            except: new Set<Room>(request.opts.except),
          };
          // 소켓 연결 종료
          return super.disconnectSockets(opts, request.close);
        }

        socket = this.nsp.sockets.get(request.sid);
        if (!socket) {
          return;
        }

        // 소켓 연결 종료
        socket.disconnect(request.close);

        response = JSON.stringify({
          requestId: request.requestId,
        });

        this.publishResponse(request, response);
        break;

      case RequestType.REMOTE_FETCH:
        if (this.requests.has(request.requestId)) {
          return;
        }

        const opts = {
          rooms: new Set<Room>(request.opts.rooms),
          except: new Set<Room>(request.opts.except),
        };
        // 매칭되는 소켓 찾기
        const localSockets = await super.fetchSockets(opts);

        response = JSON.stringify({
          requestId: request.requestId,
          sockets: localSockets.map((socket) => {
            // remove sessionStore from handshake, as it may contain circular references
            const { sessionStore, ...handshake } = socket.handshake;
            return {
              id: socket.id,
              handshake,
              rooms: [...socket.rooms],
              data: socket.data,
            };
          }),
        });

        this.publishResponse(request, response);
        break;

      case RequestType.SERVER_SIDE_EMIT:
        if (request.uid === this.uid) {
          debug("ignore same uid");
          return;
        }
        const withAck = request.requestId !== undefined;
        if (!withAck) {
          this.nsp._onServerSideEmit(request.data);
          return;
        }
        let called = false;
        const callback = (arg) => {
          // only one argument is expected
          if (called) {
            return;
          }
          called = true;
          debug("calling acknowledgement with %j", arg);
          this.pubClient.publish(
            this.responseChannel,
            JSON.stringify({
              type: RequestType.SERVER_SIDE_EMIT,
              requestId: request.requestId,
              data: arg,
            })
          );
        };
        request.data.push(callback);
        this.nsp._onServerSideEmit(request.data);
        break;

      case RequestType.BROADCAST: {
        if (this.ackRequests.has(request.requestId)) {
          // ignore self
          return;
        }

        const opts = {
          rooms: new Set<Room>(request.opts.rooms),
          except: new Set<Room>(request.opts.except),
        };

        super.broadcastWithAck(
          request.packet,
          opts,
          (clientCount) => {
            debug("waiting for %d client acknowledgements", clientCount);
            this.publishResponse(
              request,
              JSON.stringify({
                type: RequestType.BROADCAST_CLIENT_COUNT,
                requestId: request.requestId,
                clientCount,
              })
            );
          },
          (arg) => {
            debug("received acknowledgement with value %j", arg);

            this.publishResponse(
              request,
              this.parser.encode({
                type: RequestType.BROADCAST_ACK,
                requestId: request.requestId,
                packet: arg,
              })
            );
          }
        );
        break;
      }

      default:
        debug("ignoring unknown request type: %s", request.type);
    }
  }

  /**
   * redis.set() -> onrequest() -> publishResponse()
   * Send the response to the requesting node
   * @param request
   * @param response
   * @private
   */
  private publishResponse(request, response) {
    const responseChannel = this.publishOnSpecificResponseChannel
      ? `${this.responseChannel}${request.uid}#`
      : this.responseChannel;
    debug("publishing response to channel %s", responseChannel);

    // 레디스에 메시지 전송
    this.pubClient.publish(responseChannel, response);
  }

  /**
   * Called on response from another node
   * 다른 노드에서 메시지 받기
   * @private
   */
  private onresponse(channel, msg) {
    let response;

    try {
      // if the buffer starts with a "{" character
      if (msg[0] === 0x7b) {
        response = JSON.parse(msg.toString());
      } else {
        response = this.parser.decode(msg);
      }
    } catch (err) {
      debug("ignoring malformed response");
      return;
    }

    const requestId = response.requestId;

    if (this.ackRequests.has(requestId)) {
      const ackRequest = this.ackRequests.get(requestId);

      switch (response.type) {
        case RequestType.BROADCAST_CLIENT_COUNT: {
          ackRequest?.clientCountCallback(response.clientCount);
          break;
        }

        case RequestType.BROADCAST_ACK: {
          ackRequest?.ack(response.packet);
          break;
        }
      }
      return;
    }

    if (
      !requestId ||
      !(this.requests.has(requestId) || this.ackRequests.has(requestId))
    ) {
      debug("ignoring unknown request");
      return;
    }

    debug("received response %j", response);

    const request = this.requests.get(requestId);

    switch (request.type) {
      case RequestType.SOCKETS:
      case RequestType.REMOTE_FETCH:
        request.msgCount++;

        // ignore if response does not contain 'sockets' key
        if (!response.sockets || !Array.isArray(response.sockets)) return;

        if (request.type === RequestType.SOCKETS) {
          response.sockets.forEach((s) => request.sockets.add(s));
        } else {
          response.sockets.forEach((s) => request.sockets.push(s));
        }

        if (request.msgCount === request.numSub) {
          clearTimeout(request.timeout);
          if (request.resolve) {
            request.resolve(request.sockets);
          }
          this.requests.delete(requestId);
        }
        break;

      case RequestType.ALL_ROOMS:
        request.msgCount++;

        // ignore if response does not contain 'rooms' key
        if (!response.rooms || !Array.isArray(response.rooms)) return;

        response.rooms.forEach((s) => request.rooms.add(s));

        if (request.msgCount === request.numSub) {
          clearTimeout(request.timeout);
          if (request.resolve) {
            request.resolve(request.rooms);
          }
          this.requests.delete(requestId);
        }
        break;

      case RequestType.REMOTE_JOIN:
      case RequestType.REMOTE_LEAVE:
      case RequestType.REMOTE_DISCONNECT:
        clearTimeout(request.timeout);
        if (request.resolve) {
          request.resolve();
        }
        this.requests.delete(requestId);
        break;

      case RequestType.SERVER_SIDE_EMIT:
        request.responses.push(response.data);

        debug(
          "serverSideEmit: got %d responses out of %d",
          request.responses.length,
          request.numSub
        );
        if (request.responses.length === request.numSub) {
          clearTimeout(request.timeout);
          if (request.resolve) {
            request.resolve(null, request.responses);
          }
          this.requests.delete(requestId);
        }
        break;

      default:
        debug("ignoring unknown request type: %s", request.type);
    }
  }

  /**
   * Broadcasts a packet.
   *
   * @param {Object} packet - packet to emit
   * @param {Object} opts - options
   *
   * @public
   */
  public broadcast(packet: any, opts: BroadcastOptions) {
    packet.nsp = this.nsp.name;

    const onlyLocal = opts && opts.flags && opts.flags.local;

    if (!onlyLocal) {
      const rawOpts = {
        rooms: [...opts.rooms],
        except: [...new Set(opts.except)],
        flags: opts.flags,
      };
      const msg = this.parser.encode([this.uid, packet, rawOpts]);
      let channel = this.channel;
      if (opts.rooms && opts.rooms.size === 1) {
        channel += opts.rooms.keys().next().value + "#";
      }
      debug("publishing message to channel %s", channel);
      this.pubClient.publish(channel, msg);
    }
    super.broadcast(packet, opts);
  }

  public broadcastWithAck(
    packet: any,
    opts: BroadcastOptions,
    clientCountCallback: (clientCount: number) => void,
    ack: (...args: any[]) => void
  ) {
    packet.nsp = this.nsp.name;

    const onlyLocal = opts?.flags?.local;

    if (!onlyLocal) {
      const requestId = uid2(6);

      const rawOpts = {
        rooms: [...opts.rooms],
        except: [...new Set(opts.except)],
        flags: opts.flags,
      };

      const request = this.parser.encode({
        uid: this.uid,
        requestId,
        type: RequestType.BROADCAST,
        packet,
        opts: rawOpts,
      });

      this.pubClient.publish(this.requestChannel, request);

      this.ackRequests.set(requestId, {
        clientCountCallback,
        ack,
      });

      // we have no way to know at this level whether the server has received an acknowledgement from each client, so we
      // will simply clean up the ackRequests map after the given delay
      setTimeout(() => {
        this.ackRequests.delete(requestId);
      }, opts.flags!.timeout);
    }

    super.broadcastWithAck(packet, opts, clientCountCallback, ack);
  }

  /**
   * Gets the list of all rooms (across every node)
   *
   * @public
   */
  public async allRooms(): Promise<Set<Room>> {
    const localRooms = new Set(this.rooms.keys());
    const numSub = await this.getNumSub();
    debug('waiting for %d responses to "allRooms" request', numSub);

    if (numSub <= 1) {
      return localRooms;
    }

    const requestId = uid2(6);
    const request = JSON.stringify({
      uid: this.uid,
      requestId,
      type: RequestType.ALL_ROOMS,
    });

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (this.requests.has(requestId)) {
          reject(
            new Error("timeout reached while waiting for allRooms response")
          );
          this.requests.delete(requestId);
        }
      }, this.requestsTimeout);

      this.requests.set(requestId, {
        type: RequestType.ALL_ROOMS,
        numSub,
        resolve,
        timeout,
        msgCount: 1,
        rooms: localRooms,
      });

      this.pubClient.publish(this.requestChannel, request);
    });
  }

  public async fetchSockets(opts: BroadcastOptions): Promise<any[]> {
    const localSockets = await super.fetchSockets(opts);

    if (opts.flags?.local) {
      return localSockets;
    }

    const numSub = await this.getNumSub();
    debug('waiting for %d responses to "fetchSockets" request', numSub);

    if (numSub <= 1) {
      return localSockets;
    }

    const requestId = uid2(6);

    const request = JSON.stringify({
      uid: this.uid,
      requestId,
      type: RequestType.REMOTE_FETCH,
      opts: {
        rooms: [...opts.rooms],
        except: [...opts.except],
      },
    });

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (this.requests.has(requestId)) {
          reject(
            new Error("timeout reached while waiting for fetchSockets response")
          );
          this.requests.delete(requestId);
        }
      }, this.requestsTimeout);

      this.requests.set(requestId, {
        type: RequestType.REMOTE_FETCH,
        numSub,
        resolve,
        timeout,
        msgCount: 1,
        sockets: localSockets,
      });

      this.pubClient.publish(this.requestChannel, request);
    });
  }

  public addSockets(opts: BroadcastOptions, rooms: Room[]) {
    if (opts.flags?.local) {
      return super.addSockets(opts, rooms);
    }

    const request = JSON.stringify({
      uid: this.uid,
      type: RequestType.REMOTE_JOIN,
      opts: {
        rooms: [...opts.rooms],
        except: [...opts.except],
      },
      rooms: [...rooms],
    });

    this.pubClient.publish(this.requestChannel, request);
  }

  public delSockets(opts: BroadcastOptions, rooms: Room[]) {
    if (opts.flags?.local) {
      return super.delSockets(opts, rooms);
    }

    const request = JSON.stringify({
      uid: this.uid,
      type: RequestType.REMOTE_LEAVE,
      opts: {
        rooms: [...opts.rooms],
        except: [...opts.except],
      },
      rooms: [...rooms],
    });

    this.pubClient.publish(this.requestChannel, request);
  }

  public disconnectSockets(opts: BroadcastOptions, close: boolean) {
    if (opts.flags?.local) {
      return super.disconnectSockets(opts, close);
    }

    const request = JSON.stringify({
      uid: this.uid,
      type: RequestType.REMOTE_DISCONNECT,
      opts: {
        rooms: [...opts.rooms],
        except: [...opts.except],
      },
      close,
    });

    this.pubClient.publish(this.requestChannel, request);
  }

  public serverSideEmit(packet: any[]): void {
    const withAck = typeof packet[packet.length - 1] === "function";

    if (withAck) {
      this.serverSideEmitWithAck(packet).catch(() => {
        // ignore errors
      });
      return;
    }

    const request = JSON.stringify({
      uid: this.uid,
      type: RequestType.SERVER_SIDE_EMIT,
      data: packet,
    });

    this.pubClient.publish(this.requestChannel, request);
  }

  private async serverSideEmitWithAck(packet: any[]) {
    const ack = packet.pop();
    const numSub = (await this.getNumSub()) - 1; // ignore self

    debug('waiting for %d responses to "serverSideEmit" request', numSub);

    if (numSub <= 0) {
      return ack(null, []);
    }

    const requestId = uid2(6);
    const request = JSON.stringify({
      uid: this.uid,
      requestId, // the presence of this attribute defines whether an acknowledgement is needed
      type: RequestType.SERVER_SIDE_EMIT,
      data: packet,
    });

    const timeout = setTimeout(() => {
      const storedRequest = this.requests.get(requestId);
      if (storedRequest) {
        ack(
          new Error(
            `timeout reached: only ${storedRequest.responses.length} responses received out of ${storedRequest.numSub}`
          ),
          storedRequest.responses
        );
        this.requests.delete(requestId);
      }
    }, this.requestsTimeout);

    this.requests.set(requestId, {
      type: RequestType.SERVER_SIDE_EMIT,
      numSub,
      timeout,
      resolve: ack,
      responses: [],
    });

    this.pubClient.publish(this.requestChannel, request);
  }

  /**
   * Get the number of subscribers of the request channel
   *
   * @private
   */

  private getNumSub(): Promise<number> {
    if (
      this.pubClient.constructor.name === "Cluster" ||
      this.pubClient.isCluster
    ) {
      // Cluster
      const nodes = this.pubClient.nodes();
      return Promise.all(
        nodes.map((node) =>
          node.send_command("pubsub", ["numsub", this.requestChannel])
        )
      ).then((values) => {
        let numSub = 0;
        values.map((value) => {
          numSub += parseInt(value[1], 10);
        });
        return numSub;
      });
    } else if (typeof this.pubClient.pSubscribe === "function") {
      return this.pubClient
        .sendCommand(["pubsub", "numsub", this.requestChannel])
        .then((res) => parseInt(res[1], 10));
    } else {
      // RedisClient or Redis
      return new Promise((resolve, reject) => {
        this.pubClient.send_command(
          "pubsub",
          ["numsub", this.requestChannel],
          (err, numSub) => {
            if (err) return reject(err);
            resolve(parseInt(numSub[1], 10));
          }
        );
      });
    }
  }

  serverCount(): Promise<number> {
    return this.getNumSub();
  }

  close(): Promise<void> | void {
    const isRedisV4 = typeof this.pubClient.pSubscribe === "function";
    if (isRedisV4) {
      this.subClient.pUnsubscribe(
        this.channel + "*",
        this.redisListeners.get("psub"),
        true
      );

      // There is a bug in redis v4 when unsubscribing multiple channels at once, so we'll unsub one at a time.
      // See https://github.com/redis/node-redis/issues/2052
      this.subClient.unsubscribe(
        this.requestChannel,
        this.redisListeners.get("sub"),
        true
      );
      this.subClient.unsubscribe(
        this.responseChannel,
        this.redisListeners.get("sub"),
        true
      );
      this.subClient.unsubscribe(
        this.specificResponseChannel,
        this.redisListeners.get("sub"),
        true
      );
    } else {
      this.subClient.punsubscribe(this.channel + "*");
      this.subClient.off(
        "pmessageBuffer",
        this.redisListeners.get("pmessageBuffer")
      );

      this.subClient.unsubscribe([
        this.requestChannel,
        this.responseChannel,
        this.specificResponseChannel,
      ]);
      this.subClient.off(
        "messageBuffer",
        this.redisListeners.get("messageBuffer")
      );
    }
  }
}
