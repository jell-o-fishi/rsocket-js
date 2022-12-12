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
  MAX_REQUEST_COUNT,
  OnExtensionSubscriber,
  Requestable,
  RSocket,
  RSocketConnector
} from "rsocket-core";
import {TcpClientTransport} from "rsocket-tcp-client";
import {encodeCompositeMetadata, encodeRoute, WellKnownMimeType,} from "rsocket-composite-metadata";
import {exit} from "process";
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
        onExtension: () => {
        },
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

interface StreamControl {
  requester: Requestable & Cancellable & OnExtensionSubscriber
  promise: Promise<void>,
  cancel: () => void
}

function listenForMessages(rsocket: RSocket): StreamControl {
  let result = {
    promise: undefined,
    requester: undefined,
    cancel: undefined
  }
  result.promise = new Promise((resolve, reject) => {
    result.requester = rsocket.requestStream(
      {
        data: Buffer.from(""),
        metadata: createRoute("messages.incoming"),
      },
      MAX_REQUEST_COUNT,
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
        },
        onExtension: () => {
        },
      }
    );
    result.cancel = function () {
      result.requester.cancel();
      resolve(null);
    }
  });

  return result;
}

async function listValues(rsocket: RSocket, route?: string, data?: string) {
  return new Promise<string[]>((resolve, reject) => {
    const values: string[] = [];
    rsocket.requestStream(
      {
        data: data == undefined ? undefined : Buffer.from(data),
        metadata: createRoute(route),
      },
      MAX_REQUEST_COUNT,
      {
        onError: (e) => reject(e),
        onNext: (payload, isComplete) => {
          values.push(payload.data.toString());

          if (isComplete) {
            resolve(values);
          }
        },
        onComplete: () => {
          resolve(values);
        },
        onExtension: () => {
        },
      }
    );
  });
}

async function messagesExamples(client1: RSocket, client2: RSocket) {
  await requestResponse(
    client1,
    "message",
    '{"user":"user1", "content":"a message"}'
  );

  const listener = listenForMessages(client1);

  await requestResponse(client1, "channel.join", "channel1");

  await requestResponse(
    client1,
    "message",
    JSON.stringify({channel: "channel1", content: "a channel message"})
  );

  const channels = await listValues(client1, "channels");
  Logger.info(`channels ${channels}`);

  listener.cancel();
  await listener.promise;
}

async function filesExample(client1: RSocket) {
  const files = await listValues(client1, "files");
  Logger.info(`files ${files}`);
}

async function statisticsExample(client1: RSocket) {
  await fireAndForget(
    client1,
    "statistics",
    JSON.stringify({memory_usage: 123})
  );

}

async function main() {
  const connector1 = makeConnector();
  const connector2 = makeConnector();

  const client1 = await connector1.connect();
  const client2 = await connector2.connect();

  await requestResponse(client1, "login", "user1");
  await requestResponse(client2, "login", "user2");

  await messagesExamples(client1, client2);
  await filesExample(client1);
  await statisticsExample(client1);

}

main()
  .then(() => exit())
  .catch((error: Error) => {
    console.error(error);
    exit(1);
  });
