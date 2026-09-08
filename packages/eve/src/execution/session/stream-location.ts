export interface StreamOwner {
  readonly runId: string;
  readonly deploymentId: string;
  readonly encryptionPublicKey?: string;
}

interface StreamLocation {
  readonly owner: string | StreamOwner;
  readonly namespace?: string;
}

/** The storage adapter alone interprets opaque stream and record IDs. */
export function encodeStreamLocation(location: StreamLocation): string {
  return JSON.stringify([location.owner, location.namespace ?? null]);
}

export function decodeStreamLocation(id: string): StreamLocation {
  const value: unknown = JSON.parse(id);
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    (value[1] !== null && typeof value[1] !== "string")
  ) {
    throw new Error("Invalid session storage reference.");
  }
  const owner = value[0];
  if (typeof owner === "string" && owner.length > 0) {
    return { owner, namespace: value[1] ?? undefined };
  }
  if (
    typeof owner !== "object" ||
    owner === null ||
    typeof owner.runId !== "string" ||
    owner.runId.length === 0 ||
    typeof owner.deploymentId !== "string" ||
    owner.deploymentId.length === 0 ||
    (owner.encryptionPublicKey !== undefined && typeof owner.encryptionPublicKey !== "string")
  ) {
    throw new Error("Invalid session storage owner.");
  }
  return {
    owner: {
      runId: owner.runId,
      deploymentId: owner.deploymentId,
      ...(owner.encryptionPublicKey === undefined
        ? {}
        : { encryptionPublicKey: owner.encryptionPublicKey }),
    },
    namespace: value[1] ?? undefined,
  };
}
