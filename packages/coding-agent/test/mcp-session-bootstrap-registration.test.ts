import { describe, expect, it } from "bun:test";
import { bindPreparedExtensions } from "../src/extensibility/extensions/loader";
import type { ExtensionAPI, ExtensionFactory } from "../src/extensibility/extensions/types";
import { MCPBootstrapError, type MCPSessionBootstrap } from "../src/mcp/bootstrap";
import { claimMCPBootstrap, selectTrustedMCPBootstrap } from "../src/mcp/session-binding";

const source = import.meta.path;
const bootstrap: MCPSessionBootstrap = () => ({
	loadConfigs: async () => ({ configs: {}, sources: {}, exaApiKeys: [] }),
	admission: () => {
		throw new Error("No configured servers");
	},
	dispose: () => {},
});

async function bind(factory: ExtensionFactory) {
	return bindPreparedExtensions([{ path: source, resolvedPath: source, factory, error: null }], import.meta.dir);
}

describe("native MCP factory registration", () => {
	it("a caught duplicate poisons the whole factory rather than installing its first callback", async () => {
		const result = await bind(api => {
			api.registerMCPSessionBootstrap(bootstrap);
			try {
				api.registerMCPSessionBootstrap(bootstrap);
			} catch {}
		});
		expect(result.extensions).toHaveLength(0);
		expect(result.errors).toHaveLength(1);
		expect(() => claimMCPBootstrap(result, { extensionPath: source })).toThrow(MCPBootstrapError);
	});

	it("a failed selected factory cannot leave a selectable bootstrap", async () => {
		const result = await bind(api => {
			api.registerMCPSessionBootstrap(bootstrap);
			throw new Error("factory failed after registration");
		});
		expect(() => claimMCPBootstrap(result, { extensionPath: source })).toThrow(MCPBootstrapError);
	});

	it("late registration cannot replace an installed callback or make a failed transaction selectable", async () => {
		let captured: ExtensionAPI | undefined;
		const result = await bind(api => {
			captured = api;
			api.registerMCPSessionBootstrap(bootstrap);
		});
		const selected = selectTrustedMCPBootstrap(result, [source]);
		claimMCPBootstrap(result, selected);
		expect(() => captured?.registerMCPSessionBootstrap(bootstrap)).toThrow(MCPBootstrapError);
		expect(() => claimMCPBootstrap(result, selected)).toThrow(MCPBootstrapError);
	});

	it("mutable extension metadata cannot impersonate another native source", async () => {
		const result = await bind(api => api.registerMCPSessionBootstrap(bootstrap));
		result.extensions[0].resolvedPath = "/forged-trusted-source.ts";
		expect(() => claimMCPBootstrap(result, { extensionPath: "/forged-trusted-source.ts" })).toThrow(
			MCPBootstrapError,
		);
		expect(selectTrustedMCPBootstrap(result, [source])).toEqual({ extensionPath: source });
		claimMCPBootstrap(result, { extensionPath: source });
	});

	it("native source selection refuses ambient declarations and reuse across sessions", async () => {
		const result = await bind(api => api.registerMCPSessionBootstrap(bootstrap));
		expect(() => claimMCPBootstrap(result, undefined)).toThrow(MCPBootstrapError);
		expect(() => selectTrustedMCPBootstrap(result, ["/unrelated-extension.ts"])).toThrow(MCPBootstrapError);
		claimMCPBootstrap(result, { extensionPath: source });
		expect(() => claimMCPBootstrap(result, { extensionPath: source })).toThrow(MCPBootstrapError);
	});
});
