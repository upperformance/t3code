import { describe, expect, it } from "@effect/vitest";

import { cellApiUrl, cellProjectName, cellSocketUrl } from "./protocol.ts";

describe("cellProjectName", () => {
  it("keeps the varying tail of a long thread id under the name budget", () => {
    const threadId =
      "thread:fixture:simple:project:project:fixture:simple:1f0c4ad2-2b8c-4c2e-a1c5-2f3e1e0f9d7a:6a9c1b2e-3d4f-4a5b-8c6d-7e8f9a0b1c2d";
    const name = cellProjectName("t3-", threadId);
    expect(name.length).toBeLessThanOrEqual(48);
    expect(name.startsWith("t3-")).toBe(true);
    expect(name.endsWith("6a9c1b2e-3d4f-4a5b-8c6d-7e8f9a0b1c2d")).toBe(true);
    expect(name).toMatch(/^[a-z0-9-]+$/);
  });

  it("is deterministic and distinct per thread", () => {
    expect(cellProjectName("t3-", "thread:a")).toBe(cellProjectName("t3-", "thread:a"));
    expect(cellProjectName("t3-", "thread:a")).not.toBe(cellProjectName("t3-", "thread:b"));
  });

  it("never yields a bare prefix", () => {
    expect(cellProjectName("t3-", "::")).toBe("t3-thread");
  });
});

describe("cell urls", () => {
  it("builds the API and socket urls under the project route", () => {
    expect(cellApiUrl("https://acme.vim.app/", "t3-x", "/prompt")).toBe(
      "https://acme.vim.app/cell/t3-x/prompt",
    );
    expect(cellSocketUrl("https://acme.vim.app", "t3-x", "t3-s1")).toBe(
      "wss://acme.vim.app/cell/t3-x/ws?client=t3-s1",
    );
    expect(cellSocketUrl("http://127.0.0.1:8787", "t3-x", "c")).toBe(
      "ws://127.0.0.1:8787/cell/t3-x/ws?client=c",
    );
  });
});
