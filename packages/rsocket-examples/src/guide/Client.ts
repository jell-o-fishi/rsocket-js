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
  OnExtensionSubscriber, OnNextSubscriber, OnTerminalSubscriber,
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

const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
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


interface StreamControl {
  requester: Requestable & Cancellable & OnExtensionSubscriber
  cancel: () => void
}


interface StatisticsControl {
  requester: OnTerminalSubscriber &
    OnNextSubscriber &
    OnExtensionSubscriber &
    Requestable &
    Cancellable,
  cancel: () => void,
  setStatisticIds: (ids: string[]) => void,
  setPeriodSeconds: (seconds: number) => void
}



class ChatClient {
  constructor(private rsocket:RSocket) {
  }

  public async requestResponse(
    this,
    route?: string,
    data?: string
  ) {
    return new Promise((resolve, reject) => {
      return this.rsocket.requestResponse(
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
              `${payload.data}`
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

  public async fireAndForget(this, route?: string, data?: string) {
    return new Promise((resolve, reject) => {
      return this.rsocket.fireAndForget(
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

  public listenForMessages(this): StreamControl {
    let result = {
      requester: undefined,
      cancel: undefined
    }
    const promise = new Promise((resolve, reject) => {
      result.requester = this.rsocket.requestStream(
        {
          data: Buffer.from(""),
          metadata: createRoute("messages.incoming"),
        },
        MAX_REQUEST_COUNT,
        {
          onError: (e) => reject(e),
          onNext: (payload, isComplete) => {
            Logger.info(
              `${payload.data}`
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
      result.cancel = async function () {
        result.requester.cancel();
        resolve(null);
        await promise;
      }
    });

    return result;
  }

  public listenForStatistics(this, route?: string, data?: string): StatisticsControl {
    const result: StatisticsControl = {
      requester: undefined,
      cancel: undefined,
      setStatisticIds: undefined,
      setPeriodSeconds: undefined
    };
    const promise = new Promise((resolve, reject) => {
      result.requester = this.rsocket.requestChannel(
        {
          data: data == undefined ? undefined : Buffer.from(data),
          metadata: createRoute("statistics"),
        },
        MAX_REQUEST_COUNT,
        false,
        {
          onError: (e) => reject(e),
          onNext: (payload, isComplete) => {
            console.log(
              `${payload.data}`
            );

            result.requester.request(1);

            if (isComplete) {
              resolve(payload);
            }
          },
          onComplete: () => {
            resolve(null);
          },
          onExtension: () => {
          },
          request: (n) => {
            console.log(`request(${n})`);
            result.requester.onNext(
              {
                data: Buffer.from("Message"),
              },
              true
            );
          },
          cancel: () => {
          },
        }
      );
      result.cancel = async function () {
        result.requester.cancel();
        resolve(null);
        await promise;
      }
    });

    result.setStatisticIds = function(ids:string[]) {
      result.requester.onNext({
        data: Buffer.from(JSON.stringify({ids:ids}))
      }, false);
    }

    result.setPeriodSeconds = function(seconds:number) {
      result.requester.onNext({
        data: Buffer.from(JSON.stringify({period_seconds: seconds}))
      }, false);
    }

    return result;
  }

  public async listValues(this, route?: string, data?: string) {
    return new Promise<string[]>((resolve, reject) => {
      const values: string[] = [];
      this.rsocket.requestStream(
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
}

async function messagesExamples(client1: ChatClient, client2: ChatClient) {

  const listener1 = client1.listenForMessages();
  const listener2 = client2.listenForMessages();

  await client1.requestResponse(
    "message",
    '{"user":"user2", "content":"a message"}'
  );

  await client1.requestResponse("channel.join", "channel1");
  await client2.requestResponse("channel.join", "channel1");

  await client1.requestResponse(
    "message",
    JSON.stringify({channel: "channel1", content: "a channel message"})
  );

  const channels = await client1.listValues("channels");
  Logger.info(`channels ${channels}`);

  await sleep(1000);
  await listener1.cancel();
  await listener2.cancel();
}

async function filesExample(client1: ChatClient) {
  const files = await client1.listValues("files");
  Logger.info(`files ${files}`);
}

async function statisticsExample(client1: ChatClient) {
  await client1.fireAndForget(
    "statistics",
    JSON.stringify({memory_usage: 123})
  );

  const control = client1.listenForStatistics("statistics");

  await sleep(5000);
  control.setStatisticIds(['users']);

  await sleep(5000);

  await control.cancel();
}

async function main() {
  const connector1 = makeConnector();
  const connector2 = makeConnector();

  const client1 = new ChatClient(await connector1.connect());
  const client2 = new ChatClient(await connector2.connect());

  await client1.requestResponse("login", "user1");
  await client2.requestResponse("login", "user2");

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
