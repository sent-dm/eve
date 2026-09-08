import { describe, expect, it } from "vitest";
import { decodeStreamLocation, encodeStreamLocation } from "#execution/session/stream-location.js";

describe("session stream addresses", () => {
  it("preserves explicit owner routing independently of the namespace", () => {
    const owner = {
      runId: "owner",
      deploymentId: "owner-deployment",
      encryptionPublicKey: "public-key",
    };
    const first = encodeStreamLocation({ owner, namespace: "events" });
    const second = encodeStreamLocation({ owner, namespace: "snapshots" });
    expect(first).not.toBe(second);
    expect(decodeStreamLocation(first)).toEqual({ owner, namespace: "events" });
    expect(decodeStreamLocation(second)).toEqual({ owner, namespace: "snapshots" });
  });

  it("distinguishes a descriptor locator from already resolved public routing", () => {
    const id = encodeStreamLocation({ owner: "holder", namespace: "eve.session.resources" });
    expect(decodeStreamLocation(id)).toEqual({
      owner: "holder",
      namespace: "eve.session.resources",
    });
    expect(decodeStreamLocation(encodeStreamLocation({ owner: "holder" }))).toEqual({
      owner: "holder",
    });
  });

  it.each(
    [
      [],
      ["holder"],
      ["holder", 3],
      ["", null],
      [{ runId: "owner" }, null],
      [{ runId: "owner", deploymentId: "" }, null],
      [{ runId: "owner", deploymentId: "deployment", encryptionPublicKey: 42 }, null],
    ].map((value) => ({ value })),
  )("rejects incomplete routing rather than interpreting it as a locator: $value", ({ value }) => {
    expect(() => decodeStreamLocation(JSON.stringify(value))).toThrow("Invalid session storage");
  });
});
