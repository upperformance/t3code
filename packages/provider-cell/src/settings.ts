/**
 * Shared by the server driver and the client settings form, so it holds only browser-safe schema
 * code.
 */
import { makeProviderSettingsSchema, secretSetting, TrimmedString } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const CELL_DEFAULT_PROJECT_PREFIX = "t3-";

export const CellSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(false)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    baseUrl: TrimmedString.pipe(
      Schema.withDecodingDefault(Effect.succeed("")),
      Schema.annotateKey({
        title: "Base URL",
        description: "Origin of the tenant that hosts the cells, such as https://acme.vim.app.",
        providerSettingsForm: { placeholder: "https://<tenant>.vim.app", clearWhenEmpty: "omit" },
      }),
    ),
    token: TrimmedString.pipe(
      Schema.withDecodingDefault(Effect.succeed("")),
      secretSetting,
      Schema.annotateKey({
        title: "Access token",
        description:
          "Sent as a bearer token with every request. Leave empty while the tenant has no authentication.",
        providerSettingsForm: { control: "password", clearWhenEmpty: "omit" },
      }),
    ),
    projectPrefix: TrimmedString.pipe(
      Schema.withDecodingDefault(Effect.succeed(CELL_DEFAULT_PROJECT_PREFIX)),
      Schema.annotateKey({
        title: "Project prefix",
        description:
          "Start of the cell project name each thread creates; the rest is a hash of the thread id.",
        providerSettingsForm: { placeholder: CELL_DEFAULT_PROJECT_PREFIX, clearWhenEmpty: "omit" },
      }),
    ),
  },
  { order: ["baseUrl", "token", "projectPrefix"] },
);
export type CellSettings = typeof CellSettings.Type;
