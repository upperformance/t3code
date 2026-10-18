import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { CellSettings } from "./settings.ts";

export const cellClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("cell"),
  label: "Cell",
  settingsSchema: CellSettings,
  badgeLabel: "Beta",
  // A cell needs a tenant URL before it can do anything, so instances are added explicitly.
  hasDefaultInstance: false,
  icon: {
    viewBox: "0 0 24 24",
    fill: { light: "#1F2937", dark: "#E5E7EB" },
    paths: [
      {
        d: "M12 2 20.66 7v10L12 22 3.34 17V7Zm0 2.31L5.34 8.16v7.68L12 19.69l6.66-3.85V8.16Z",
        fillRule: "evenodd",
      },
      { d: "M12 8.5a3.5 3.5 0 1 1 0 7 3.5 3.5 0 0 1 0-7Z" },
    ],
  },
});
