/**
 * Tests for Network.match() — issue #499
 *
 * Each Network / HttpAdapter implementation must convert a plain target string
 * (e.g. "default") into a routable address. These tests verify that:
 *
 * 1. LocalNetwork.match   → `local://any@<target>`
 * 2. PollMessageSource.match → `poll://any@<target>`
 * 3. HttpNetwork.match    → delegates to its adapter
 */

import { HttpNetwork, PollMessageSource } from "../../src/network/http.js";
import { LocalNetwork } from "../../src/network/local.js";

// ---------------------------------------------------------------------------
// LocalNetwork
// ---------------------------------------------------------------------------

describe("LocalNetwork.match", () => {
  test("returns local://any@<target>", () => {
    const network = new LocalNetwork({ pid: "pid1", group: "grp" });
    expect(network.match("my-group")).toBe("local://any@my-group");
  });

  test("uses the target argument, not the network's own group", () => {
    const network = new LocalNetwork({ pid: "pid1", group: "own-group" });
    expect(network.match("other-group")).toBe("local://any@other-group");
  });

  test("handles the default target string", () => {
    const network = new LocalNetwork();
    expect(network.match("default")).toBe("local://any@default");
  });
});

// ---------------------------------------------------------------------------
// PollMessageSource
// ---------------------------------------------------------------------------

describe("PollMessageSource.match", () => {
  // PollMessageSource opens an SSE connection in the constructor; we give it
  // a URL that will immediately fail — that's fine, we only need the match method.
  function makePoll(group = "grp", pid = "pid1"): PollMessageSource {
    return new PollMessageSource({
      url: `http://localhost:0/poll/${encodeURIComponent(group)}/${encodeURIComponent(pid)}`,
    });
  }

  test("returns poll://any@<target>", () => {
    const adapter = makePoll();
    expect(adapter.match("my-group")).toBe("poll://any@my-group");
  });

  test("uses the target argument, not the adapter's own group", () => {
    const adapter = makePoll("own-group");
    expect(adapter.match("other-group")).toBe("poll://any@other-group");
  });

  test("handles the default target string", () => {
    const adapter = makePoll();
    expect(adapter.match("default")).toBe("poll://any@default");
  });
});

// ---------------------------------------------------------------------------
// HttpNetwork — delegates to the adapter
// ---------------------------------------------------------------------------

describe("HttpNetwork.match", () => {
  test("delegates to PollMessageSource.match", () => {
    const adapter = new PollMessageSource({
      url: "http://localhost:0/poll/my-group/my-pid",
    });
    const network = new HttpNetwork({ adapter });

    expect(network.match("some-target")).toBe("poll://any@some-target");
  });
});
