import { describe, expect, it } from "bun:test";
import type { MCPSessionBinding, MCPSessionBootstrapInstallation } from "../src/mcp/bootstrap";
import { MCPBootstrapError } from "../src/mcp/bootstrap";
import { getMCPManagerBinding, NativeMCPBootstrapLifecycle, isMCPSessionBinding } from "../src/mcp/session-binding";

const emptyConfigs = { configs: {}, sources: {}, exaApiKeys: [] };

describe("native MCP generation lifecycle", () => {
	it("holds native discovery until complete installation, then revokes before awaited drain", async () => {
		const installed = Promise.withResolvers<MCPSessionBootstrapInstallation>();
		const entered = Promise.withResolvers<MCPSessionBinding>();
		const releaseCleanup = Promise.withResolvers<void>();
		let loads = 0;
		let published = 0;
		let disposed = 0;
		const lifecycle = new NativeMCPBootstrapLifecycle(
			context => {
				entered.resolve(context.binding);
				return installed.promise;
			},
			() => ({ cwd: import.meta.dir, sessionId: "bootstrap-test", agentKind: "main" }),
			{},
			async (_manager, result) => {
				if (result) published++;
			},
		);
		const creation = lifecycle.initialize(true);
		const binding = await entered.promise;
		expect(loads).toBe(0);
		expect(published).toBe(0);
		installed.resolve({
			loadConfigs: async () => {
				loads++;
				return emptyConfigs;
			},
			admission: () => {
				throw new Error("Empty catalog must not authenticate");
			},
			dispose: async () => {
				disposed++;
				await releaseCleanup.promise;
			},
		});
		const manager = await creation;
		expect(loads).toBe(1);
		expect(published).toBe(1);
		expect(getMCPManagerBinding(manager)).toBe(binding);
		const retirement = lifecycle.revoke();
		expect(binding.signal.aborted).toBe(true);
		let drained = false;
		void retirement.then(() => {
			drained = true;
		});
		await Promise.resolve();
		expect(drained).toBe(false);
		releaseCleanup.resolve();
		await retirement;
		expect(disposed).toBe(1);
		await lifecycle.revoke();
		expect(disposed).toBe(1);
	});

	it("a cancelled transition never restores its old binding or previously admitted generation", async () => {
		const bindings: MCPSessionBinding[] = [];
		let sessionId = "old";
		const observedSessions: string[] = [];
		const lifecycle = new NativeMCPBootstrapLifecycle(
			context => {
				bindings.push(context.binding);
				observedSessions.push(context.sessionId);
				return {
					loadConfigs: async () => emptyConfigs,
					admission: () => {
						throw new Error("No servers");
					},
					dispose: () => {},
				};
			},
			() => ({ cwd: import.meta.dir, sessionId, agentKind: "main" }),
			{},
			async () => {},
		);
		await lifecycle.initialize(true);
		const rotating = lifecycle.rotate();
		expect(bindings[0].signal.aborted).toBe(true);
		const transition = await rotating;
		await expect(lifecycle.rotate()).rejects.toBeInstanceOf(MCPBootstrapError);
		// The conversation rollback keeps its old ID, but must get fresh authority.
		sessionId = "old";
		await transition[Symbol.asyncDispose]();
		expect(bindings).toHaveLength(2);
		expect(bindings[1]).not.toBe(bindings[0]);
		expect(bindings[1].signal.aborted).toBe(false);
		expect(observedSessions).toEqual(["old", "old"]);
		await lifecycle.revoke();
	});

	it("a late bootstrap result after cancellation is disposed without discovery or publication", async () => {
		const installed = Promise.withResolvers<MCPSessionBootstrapInstallation>();
		const entered = Promise.withResolvers<MCPSessionBinding>();
		let loads = 0;
		let disposed = 0;
		const lifecycle = new NativeMCPBootstrapLifecycle(
			context => {
				entered.resolve(context.binding);
				return installed.promise;
			},
			() => ({ cwd: import.meta.dir, sessionId: "late", agentKind: "main" }),
			{},
			async () => {},
		);
		const creation = lifecycle.initialize(true);
		const failedCreation = creation.catch((error: unknown) => error);
		const binding = await entered.promise;
		const retirement = binding.revoke();
		expect(binding.signal.aborted).toBe(true);
		installed.resolve({
			loadConfigs: async () => {
				loads++;
				return emptyConfigs;
			},
			admission: () => {
				throw new Error("No servers");
			},
			dispose: () => {
				disposed++;
			},
		});
		expect(await failedCreation).toBeInstanceOf(MCPBootstrapError);
		await retirement;
		expect(loads).toBe(0);
		expect(disposed).toBe(1);
	});

	it("failed host cleanup quarantines rotation and does not claim successful revocation", async () => {
		let bootstraps = 0;
		const lifecycle = new NativeMCPBootstrapLifecycle(
			() => {
				bootstraps++;
				return {
					loadConfigs: async () => emptyConfigs,
					admission: () => {
						throw new Error("No servers");
					},
					dispose: () => {
						throw new Error("watch cleanup failed");
					},
				};
			},
			() => ({ cwd: import.meta.dir, sessionId: "cleanup", agentKind: "main" }),
			{},
			async () => {},
		);
		const manager = await lifecycle.initialize(true);
		const binding = getMCPManagerBinding(manager);
		if (!binding) throw new Error("Missing native binding");
		expect(
			isMCPSessionBinding({ signal: binding.signal, revoke: binding.revoke, revokePromise: binding.revokePromise }),
		).toBe(false);
		await expect(lifecycle.revoke()).rejects.toBeInstanceOf(MCPBootstrapError);
		expect(binding.signal.aborted).toBe(true);
		await expect(lifecycle.rotate()).rejects.toBeInstanceOf(MCPBootstrapError);
		expect(bootstraps).toBe(1);
	});
});
