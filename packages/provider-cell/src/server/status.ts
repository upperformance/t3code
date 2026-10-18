/**
 * The status probe reads `GET state` of a dedicated probe project and never
 * posts a prompt, so a background refresh cannot start a run or spend tokens.
 */
import type { ServerProviderModel } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";

import {
  buildServerProvider,
  type ProviderProbeResult,
} from "@t3tools/provider-core/server/snapshotProbe";

import type { CellSettings } from "../settings.ts";
import {
  cellApiUrl,
  cellAuthorizationHeaders,
  cellProbeProjectName,
  readCellState,
} from "./protocol.ts";
import type { CellTransport } from "./transport.ts";

const CELL_PRESENTATION = {
  displayName: "Cell",
  badgeLabel: "Beta",
  showInteractionModeToggle: false,
  reportsContextWindow: false,
  supportsConversationRollback: false,
  // The cell is the sandbox: nothing it runs reaches the host, and it asks no approvals.
  supportedRuntimeModes: ["full-access"],
} as const;

const PROBE_TIMEOUT_MS = 8_000;

/** The tenant decides the model; the catalog is that one entry, and the adapter refuses any other. */
function cellModels(model: string | undefined): ReadonlyArray<ServerProviderModel> {
  return model
    ? [{ slug: model, name: model, isCustom: false, isDefault: true, capabilities: null }]
    : [];
}

export const makePendingCellProvider = Effect.fn("makePendingCellProvider")(function* (
  settings: CellSettings,
) {
  return buildServerProvider({
    presentation: CELL_PRESENTATION,
    enabled: settings.enabled,
    checkedAt: DateTime.formatIso(yield* DateTime.now),
    models: cellModels(undefined),
    probe: {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: settings.enabled
        ? "Checking the cell tenant..."
        : "Cell is disabled in T3 Code settings.",
    },
  });
});

export const checkCellProviderStatus = Effect.fn("checkCellProviderStatus")(function* (
  settings: CellSettings,
  transport: CellTransport,
) {
  if (!settings.enabled) return yield* makePendingCellProvider(settings);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const snapshot = (probe: ProviderProbeResult, model?: string) =>
    buildServerProvider({
      presentation: CELL_PRESENTATION,
      enabled: true,
      checkedAt,
      models: cellModels(model),
      probe,
    });
  if (!settings.baseUrl) {
    return snapshot({
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Set the tenant base URL for this cell instance in Settings → Providers.",
    });
  }
  const response = yield* transport
    .request({
      method: "GET",
      url: cellApiUrl(settings.baseUrl, cellProbeProjectName(settings.projectPrefix), "/state"),
      headers: cellAuthorizationHeaders(settings.token),
    })
    .pipe(Effect.timeoutOption(PROBE_TIMEOUT_MS), Effect.result);
  if (Result.isFailure(response)) {
    return snapshot({
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: `Could not reach the cell tenant at ${settings.baseUrl}: ${response.failure.detail}`,
    });
  }
  if (Option.isNone(response.success)) {
    return snapshot({
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: `The cell tenant at ${settings.baseUrl} did not answer in time.`,
    });
  }
  const { status, body } = response.success.value;
  if (status === 401 || status === 403) {
    return snapshot({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: "The cell tenant rejected the access token. Update it in Settings → Providers.",
    });
  }
  const state = yield* readCellState(body).pipe(Effect.result);
  if (status !== 200 || (Result.isFailure(state) && state.failure._tag === "SchemaError")) {
    return snapshot({
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: `${settings.baseUrl} answered ${status} without a cell state; it is not a cell tenant.`,
    });
  }
  // A cell tenant, but one speaking an older protocol than this provider.
  if (Result.isFailure(state)) {
    return snapshot({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: state.failure.message,
    });
  }
  const version = state.success.version ?? null;
  if (!state.success.configured) {
    return snapshot(
      {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unknown" },
        message:
          "The tenant has no model API key, so prompts will be refused until OPENAI_API_KEY is configured on celld.",
      },
      state.success.model,
    );
  }
  return snapshot(
    { installed: true, version, status: "ready", auth: { status: "unknown" } },
    state.success.model,
  );
});
