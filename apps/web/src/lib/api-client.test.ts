import { describe, expect, it } from "vitest";

import { resolveServerBaseUrl } from "./api-client";

describe("resolveServerBaseUrl", () => {
  it("prefers an explicit public base URL when present", () => {
    expect(
      resolveServerBaseUrl({
        NEXT_PUBLIC_API_BASE_URL: "http://localhost:3300/",
        PORT: "3000",
      })
    ).toBe("http://localhost:3300");
  });

  it("falls back to the runtime port when no explicit base URL is set", () => {
    expect(
      resolveServerBaseUrl({
        PORT: "3300",
      })
    ).toBe("http://127.0.0.1:3300");
  });

  it("uses WEB_PORT when PORT is not set", () => {
    expect(
      resolveServerBaseUrl({
        WEB_PORT: "3200",
      })
    ).toBe("http://127.0.0.1:3200");
  });
});
