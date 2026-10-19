import { ProviderDriverKind } from "@t3tools/contracts";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";

import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";
import type { ProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenance";

import { CellSettings } from "../settings.ts";
import { makeCellAdapterV2 } from "./adapter.ts";
import { checkCellProviderStatus, makePendingCellProvider } from "./status.ts";
import { cellTextGeneration } from "./textGeneration.ts";
import { makeHttpCellTransport } from "./transport.ts";

const DRIVER_KIND = ProviderDriverKind.make("cell");
const decodeCellSettings = Schema.decodeSync(CellSettings);

/** Nothing on the environment to update: the cell runs on the tenant. */
const MAINTENANCE: ProviderMaintenanceCapabilities = {
  provider: DRIVER_KIND,
  packageName: null,
  update: null,
  latestVersion: null,
};

export type CellAdapterV2DriverEnv =
  | IdAllocator.IdAllocatorV2
  | HttpClient.HttpClient
  | ProviderEventLoggers.ProviderEventLoggers;

export type CellDriverEnv = CellAdapterV2DriverEnv | ProviderHost.ProviderHost;

const makeTransport = makeHttpCellTransport.pipe(
  // The ws-backed constructor carries the bearer token on the handshake; the global one cannot.
  Effect.provide(NodeSocket.layerWebSocketConstructorWS),
);

export const CellAdapterV2Driver: ProviderAdapterDriver<CellSettings, CellAdapterV2DriverEnv> = {
  driverKind: DRIVER_KIND,
  configSchema: CellSettings,
  defaultConfig: () => decodeCellSettings({}),
  create: ({ instanceId, enabled, config }) =>
    Effect.gen(function* () {
      const eventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      const transport = yield* makeTransport;
      return yield* makeCellAdapterV2({
        instanceId,
        settings: { ...config, enabled },
        transport,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
        continuationRequests,
      });
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterDriverCreateError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build the cell orchestration adapter.",
            cause,
          }),
      ),
    ),
};

export const CellDriver: ProviderDriver<CellSettings, CellDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: { displayName: "Cell", supportsMultipleInstances: true, hasDefaultInstance: false },
  configSchema: CellSettings,
  defaultConfig: () => decodeCellSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const effectiveConfig = { ...config, enabled } satisfies CellSettings;
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const transport = yield* makeTransport;
      const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<CellSettings>>({
        resolveMaintenance: () => Effect.succeed(MAINTENANCE),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingCellProvider(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkCellProviderStatus(effectiveConfig, transport).pipe(
          Effect.map(stampIdentity),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build the cell snapshot.",
              cause,
            }),
        ),
      );
      const orchestrationAdapter = yield* CellAdapterV2Driver.create({
        instanceId,
        displayName,
        accentColor,
        environment,
        enabled,
        config,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build the cell orchestration adapter.",
              cause,
            }),
        ),
      );
      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration: cellTextGeneration,
      } satisfies ProviderInstance;
    }),
};
