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

import { RSocket, RSocketConnector } from "rsocket-core";
import { TcpClientTransport } from "rsocket-tcp-client";
import {
  encodeCompositeMetadata,
  encodeRoute,
  WellKnownMimeType,
} from "rsocket-composite-metadata";
import { exit } from "process";
import Logger from "../shared/logger";
import MESSAGE_RSOCKET_ROUTING = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;

function makeConnector() {
  return new RSocketConnector({
    setup: {
      metadataMimeType: "message/x.rsocket.composite-metadata.v0",
    },
    transport: new TcpClientTransport({
      connectionOptions: {
        host: "127.0.0.1",
        port: 6565,
      },
    }),
  });
}

function createRoute(route?: string) {
  let compositeMetaData = undefined;
  if (route) {
    const encodedRoute = encodeRoute(route);

    const map = new Map<WellKnownMimeType, Buffer>();
    map.set(MESSAGE_RSOCKET_ROUTING, encodedRoute);
    compositeMetaData = encodeCompositeMetadata(map);
  }
  return compositeMetaData;
}

async function requestResponse(
  rsocket: RSocket,
  route?: string,
  data?: string
) {
  return new Promise((resolve, reject) => {
    return rsocket.requestResponse(
      {
        data: Buffer.from(data),
        metadata: createRoute(route),
      },
      {
        onError: (e) => {
          reject(e);
        },
        onNext: (payload, isComplete) => {
          Logger.info(
            `payload[data: ${payload.data}; metadata: ${payload.metadata}]|${isComplete}`
          );
          resolve(payload);
        },
        onComplete: () => {
          resolve({});
        },
        onExtension: () => {},
      }
    );
  });
}

async function fireAndForget(rsocket: RSocket, route?: string, data?: string) {
  return new Promise((resolve, reject) => {
    return rsocket.fireAndForget(
      {
        data: Buffer.from(data),
        metadata: createRoute(route),
      },
      {
        onError: (error) => {
          reject(error);
        },
        onComplete: () => {
          resolve(null);
        },
      }
    );
  });
}

function listenForMessages(rsocket: RSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    const requester = rsocket.requestStream(
      {
        data: Buffer.from(""),
        metadata: createRoute("messages.incoming"),
      },
      10000,
      {
        onError: (e) => reject(e),
        onNext: (payload, isComplete) => {
          Logger.info(
            `[client] payload[data: ${payload.data}; metadata: ${payload.metadata}]|isComplete: ${isComplete}`
          );

          if (isComplete) {
            resolve(null);
          }
        },
        onComplete: () => {
          resolve(null);
          requester.cancel();
        },
        onExtension: () => {},
      }
    );
  });
}

async function main() {
  const connector = makeConnector();

  const rsocket = await connector.connect();

  await requestResponse(rsocket, "login", "user1");

  await requestResponse(
    rsocket,
    "message",
    '{"user":"user1", "content":"a message"}'
  );

  const listener = listenForMessages(rsocket);

  await requestResponse(rsocket, "channel.join", "channel1");

  await requestResponse(
    rsocket,
    "message",
    JSON.stringify({ channel: "channel1", content: "a channel message" })
  );

  await fireAndForget(
    rsocket,
    "statistics",
    JSON.stringify({ memory_usage: 123 })
  );

  await listener;
}

main()
  .then(() => exit())
  .catch((error: Error) => {
    console.error(error);
    exit(1);
  });
