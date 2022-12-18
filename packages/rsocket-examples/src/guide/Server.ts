/*
 * Copyright 2021-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Cancellable,
  ErrorCodes,
  OnExtensionSubscriber,
  OnNextSubscriber,
  OnTerminalSubscriber,
  Payload,
  Requestable,
  RSocketError,
  RSocketServer,
} from "rsocket-core";
import {TcpServerTransport} from "rsocket-tcp-server";
import {
  decodeCompositeMetadata,
  decodeRoutes,
  WellKnownMimeType,
} from "rsocket-composite-metadata";
import {exit} from "process";
import {queue} from "async";
import MESSAGE_RSOCKET_ROUTING = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
import Logger from "../shared/logger";

let serverCloseable;

function mapMetaData(payload: Payload) {
  const mappedMetaData = new Map<string, any>();
  if (payload.metadata) {
    const decodedCompositeMetaData = decodeCompositeMetadata(payload.metadata);

    for (let metaData of decodedCompositeMetaData) {
      switch (metaData.mimeType) {
        case MESSAGE_RSOCKET_ROUTING.toString(): {
          const tags = [];
          for (let decodedRoute of decodeRoutes(metaData.content)) {
            tags.push(decodedRoute);
          }
          const joinedRoute = tags.join(".");
          mappedMetaData.set(MESSAGE_RSOCKET_ROUTING.toString(), joinedRoute);
        }
      }
    }
  }
  return mappedMetaData;
}

class ChatChannel {
  name: string;
  users: Set<string> = new Set<string>();
  messages: queue;
}

class ChatServiceSession {
  sessionId: string;
  username: string;
  messages: queue;
  messageListeningStream: OnNextSubscriber;

  constructor(private shared: ChatServiceShared) {
  }

  handleLogin(
    responderStream: OnTerminalSubscriber &
      OnNextSubscriber &
      OnExtensionSubscriber,
    payload: Payload
  ) {
    const timeout = setTimeout(() => {
      this.username = payload.data.toString("utf8");
      this.sessionId = payload.data.toString("utf8"); // todo replace with uuid4 generation
      this.shared.sessionById.set(this.sessionId, this);

      const self = this;
      this.messages = queue(function (message, callback) {
        self.messageListeningStream.onNext(
          {
            data: Buffer.from(JSON.stringify(message)),
          },
          false
        );
      });
      this.messages.pause();
      return responderStream.onNext(
        {
          data: Buffer.concat([Buffer.from("Echo: "), payload.data]),
        },
        true
      );
    }, 1000);
    return {
      cancel: () => {
        clearTimeout(timeout);
      },
      onExtension: () => {
      },
    };
  }

  sendMessage(
    responderStream: OnTerminalSubscriber &
      OnNextSubscriber &
      OnExtensionSubscriber,
    payload: Payload
  ) {
    const timeout = setTimeout(() => {
      const message = payload.data.toString("utf8");
      this.sessionId = payload.data.toString("utf8"); // todo replace with uuid4 generation
      return responderStream.onNext({data: null}, true);
    }, 1000);

    return {
      cancel: () => {
        clearTimeout(timeout);
      },
      onExtension: () => {
      },
    };
  }

  joinChannel(
    responderStream: OnTerminalSubscriber &
      OnNextSubscriber &
      OnExtensionSubscriber,
    payload: Payload
  ) {
    const timeout = setTimeout(() => {
      const channelName = payload.data.toString();
      ensureChannel(channelName, this.shared);
      this.shared.channelsByName.get(channelName).users.add(this.sessionId);
      return responderStream.onNext({data: null}, true);
    }, 1000);
    return {
      cancel: () => {
        clearTimeout(timeout);
      },
      onExtension: () => {
      },
    };
  }

  leaveChannel(
    responderStream: OnTerminalSubscriber &
      OnNextSubscriber &
      OnExtensionSubscriber,
    payload: Payload
  ) {
    const timeout = setTimeout(() => {
      const channelName = payload.data.toString();
      this.shared.channelsByName.get(channelName).users.delete(this.sessionId);
      return responderStream.onNext({data: null}, true);
    }, 1000);
    return {
      cancel: () => {
        clearTimeout(timeout);
      },
      onExtension: () => {
      },
    };
  }
}

class ChatServiceShared {
  sessionById: Map<string, ChatServiceSession> = new Map<string, ChatServiceSession>();
  channelsByName: Map<string, ChatChannel> = new Map<string, ChatChannel>();
}

function ensureChannel(channelName: string, shared: ChatServiceShared) {
  if (!shared.channelsByName.has(channelName)) {
    const channel = new ChatChannel();
    channel.name = channelName;
    shared.channelsByName.set(channelName, channel);

    channel.messages = queue(function (message, callback) {
      for (const userSessionId of channel.users) {
        const user = shared.sessionById.get(userSessionId);
        user.messages.push(message);
      }
    });
  }
}

function makeServer() {
  const chatService = new ChatServiceShared();
  return new RSocketServer({
    transport: new TcpServerTransport({
      listenOptions: {
        port: 9090,
        host: "127.0.0.1",
      },
    }),
    acceptor: {
      accept: async () => {
        const chatServiceSession = new ChatServiceSession(chatService);
        return {
          fireAndForget(
            payload: Payload,
            responderStream: OnTerminalSubscriber
          ): Cancellable {
            return {
              cancel(): void {
              }
            }
          },
          requestResponse: (
            payload: Payload,
            responderStream: OnTerminalSubscriber &
              OnNextSubscriber &
              OnExtensionSubscriber
          ) => {
            const mappedMetaData = mapMetaData(payload);

            const defaultSubscriber = {
              cancel() {
                console.log("cancelled");
              },
              onExtension() {
              },
            };

            const route = mappedMetaData.get(
              MESSAGE_RSOCKET_ROUTING.toString()
            );
            if (!route) {
              responderStream.onError(
                new RSocketError(
                  ErrorCodes.REJECTED,
                  "Composite metadata did not include routing information."
                )
              );
              return defaultSubscriber;
            }

            switch (route) {
              case "login": {
                return chatServiceSession.handleLogin(responderStream, payload);
              }
              case "channel.join": {
                return chatServiceSession.joinChannel(responderStream, payload);
              }
              case "channel.leave": {
                return chatServiceSession.leaveChannel(responderStream, payload);
              }
              case "message": {
                return chatServiceSession.sendMessage(responderStream, payload);
              }
              default: {
                responderStream.onError(
                  new RSocketError(
                    ErrorCodes.REJECTED,
                    "The encoded route was unknown by the server."
                  )
                );
                return defaultSubscriber;
              }
            }
          },
          requestStream(
            payload: Payload,
            initialRequestN: number,
            responderStream: OnTerminalSubscriber &
              OnNextSubscriber &
              OnExtensionSubscriber
          ): Requestable & Cancellable & OnExtensionSubscriber {
            Logger.info(
              `[server] requestStream payload[data: ${payload.data}; metadata: ${payload.metadata}]|initialRequestN: ${initialRequestN}`
            );

            const mappedMetaData = mapMetaData(payload);

            const defaultSubscriber = {
              cancel() {
                console.log("cancelled");
              },
              request(n) {
              },
              onExtension() {
              },
            };

            const route = mappedMetaData.get(
              MESSAGE_RSOCKET_ROUTING.toString()
            );
            if (!route) {
              responderStream.onError(
                new RSocketError(
                  ErrorCodes.REJECTED,
                  "Composite metadata did not include routing information."
                )
              );
              return defaultSubscriber;
            }

            switch (route) {
              case "messages.incoming": {
                let interval = null;
                let requestedResponses = initialRequestN;
                let sentResponses = 0;

                // simulate async data with interval
                interval = setInterval(() => {
                  sentResponses++;
                  let isComplete = sentResponses >= requestedResponses;
                  responderStream.onNext(
                    {
                      data: Buffer.from(new Date()),
                      metadata: undefined,
                    },
                    isComplete
                  );
                  if (isComplete) {
                    clearInterval(interval);
                  }
                }, 750);

                return {
                  cancel() {
                    Logger.info("[server] stream cancelled by client");
                    clearInterval(interval);
                  },
                  request(n) {
                    requestedResponses += n;
                    Logger.info(
                      `[server] request n: ${n}, requestedResponses: ${requestedResponses}, sentResponses: ${sentResponses}`
                    );
                  },
                  onExtension: () => {
                  },
                };
              }
              default:
                return {
                  cancel() {
                    Logger.info("[server] stream cancelled by client");
                  },
                  request(n) {
                  },
                  onExtension: () => {
                  },
                };
            }
          },
          requestChannel: (
            payload: Payload,
            initialRequestN: number,
            isCompleted: boolean,
            responderStream: OnTerminalSubscriber &
              OnNextSubscriber &
              OnExtensionSubscriber &
              Requestable &
              Cancellable
          ): OnTerminalSubscriber &
            OnNextSubscriber &
            OnExtensionSubscriber &
            Requestable &
            Cancellable => {
            responderStream.onNext(payload, isCompleted);
            responderStream.request(initialRequestN);

            return {
              cancel(): void {
                responderStream.cancel();
              },
              onComplete(): void {
                responderStream.onComplete();
              },
              onError(error: Error): void {
                responderStream.onError(error);
              },
              onExtension(): void {
              },
              onNext(payload: Payload, isComplete: boolean): void {
                setTimeout(
                  () => responderStream.onNext(payload, isComplete),
                  10
                );
              },
              request(requestN: number): void {
                setTimeout(() => responderStream.request(requestN), 1);
              },
            };
          },
        };
      },
    },
  });
}

async function main() {
  const server = makeServer();

  serverCloseable = await server.bind();
}

main().catch((error: Error) => {
  console.error(error);
  exit(1);
});
