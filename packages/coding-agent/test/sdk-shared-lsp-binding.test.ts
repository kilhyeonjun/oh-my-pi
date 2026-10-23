import { afterEach, describe, expect, it, vi } from "bun:test";
import type { Api, ModelSpec } from "@oh-my-pi/pi-ai";
import { buildModel } from "@oh-my-pi/pi-catalog/build";
import { resetCapabilityForTests } from "@oh-my-pi/pi-coding-agent/capability";
import { ModelRegistry } from "@oh-my-pi/pi-coding-agent/config/model-registry";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import * as lspClient from "@oh-my-pi/pi-coding-agent/lsp/client";
import type { LspBeforeEditEvent } from "@oh-my-pi/pi-coding-agent/extensibility/extensions/types";
import { cfgLspShared } from "@oh-my-pi/pi-coding-agent/lsp/settings";
import { type CreateAgentSessionOptions, createAgentSession } from "@oh-my-pi/pi-coding-agent/sdk";
import type { AgentSession } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { AuthStorage } from "@oh-my-pi/pi-coding-agent/session/auth-storage";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { createSubagentSettings } from "@oh-my-pi/pi-coding-agent/task/executor";
import { BUILTIN_TOOLS, type ToolSession } from "@oh-my-pi/pi-coding-agent/tools";
import { TempDir } from "@oh-my-pi/pi-utils";

const modelSpec: ModelSpec<Api> = {
	id: "shared-lsp-binding",
	name: "Shared LSP binding",
	api: "test-shared-lsp-binding",
	provider: "managed-primary",
	baseUrl: "http://127.0.0.1:8080/v1",
	reasoning: false,
	input: ["text"],
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	contextWindow: 32_768,
	maxTokens: 1024,
};
const model = buildModel(modelSpec);

// The shared-LSP flag is process-global and read on every client cold start;
// subagent and helper sessions must not switch it off for the parent.
describe("shared LSP flag with subagents and helper sessions", () => {
	const sessions: AgentSession[] = [];

	afterEach(async () => {
		for (const session of sessions.splice(0)) {
			if (!session.isDisposed) await session.dispose();
		}
		vi.restoreAllMocks();
		lspClient.setSharedLspEnabled(false);
		resetCapabilityForTests();
	});

	const starter = async (tempDir: TempDir) => {
		const authStorage = await AuthStorage.create(tempDir.join("auth.db"));
		authStorage.keys.setRuntime(model.provider, "test-key");
		const modelRegistry = new ModelRegistry(authStorage, tempDir.join("models.yml"));
		const start = async (
			settings: Settings,
			extra: Pick<
				CreateAgentSessionOptions,
				"parentTaskPrefix" | "taskDepth" | "agentId" | "bindProcessState" | "enableLsp" | "extensions"
			> = {},
		): Promise<AgentSession> => {
			const { session } = await createAgentSession({
				cwd: tempDir.path(),
				agentDir: tempDir.path(),
				sessionManager: SessionManager.inMemory(tempDir.path()),
				authStorage,
				modelRegistry,
				settings,
				model,
				disableExtensionDiscovery: true,
				skills: [],
				contextFiles: [],
				promptTemplates: [],
				slashCommands: [],
				rules: [],
				enableMCP: false,
				enableLsp: false,
				skipPythonPreflight: true,
				...extra,
			});
			sessions.push(session);
			return session;
		};
		return { start, close: () => authStorage.close() };
	};

	it("memoizes protected session policies, isolates scopes, and denies late effects", async () => {
		using tempDir = TempDir.createSync("@pi-lsp-policy-binding-");
		const { start, close } = await starter(tempDir);
		let captured: ToolSession | undefined;
		const originalRead = BUILTIN_TOOLS.read;
		vi.spyOn(BUILTIN_TOOLS, "read").mockImplementation(toolSession => {
			captured = toolSession;
			return originalRead(toolSession);
		});
		const shutdown = vi.spyOn(lspClient, "shutdownScopedClients").mockResolvedValue();
		const settings = Settings.isolated({ "compaction.enabled": false });
		try {
			await start(settings);
			expect(captured?.getLspPolicy?.()).toBeUndefined();
			const edit = { origin: "rename" as const, cwd: tempDir.path(), changes: [] };
			const execute = { purpose: "server" as const, command: "/bin/server", args: [], cwd: tempDir.path() };
			const beforeEdit = vi.fn((_event: LspBeforeEditEvent) => ({ block: true, reason: "edits denied" }));
			const first = await start(settings, {
				extensions: [
					api => {
						api.on("lsp_before_edit", beforeEdit);
					},
				],
			});
			const firstToolSession = captured!;
			const policy = firstToolSession.getLspPolicy!()!;
			expect(policy).toBeDefined();
			expect(firstToolSession.getLspPolicy!()).toBe(policy);
			await policy.beforeExecute(execute);
			await expect(policy.beforeEdit(edit)).rejects.toThrow("edits denied");
			expect(beforeEdit).toHaveBeenCalledTimes(1);
			expect(beforeEdit.mock.calls[0]?.[0]).toEqual({ ...edit, type: "lsp_before_edit" });
			await start(settings, {
				extensions: [
					api => {
						api.on("lsp_before_execute", event => {
							expect(event).toEqual({ ...execute, type: "lsp_before_execute" });
							return { block: true, reason: "execution denied" };
						});
					},
				],
			});
			const secondPolicy = captured!.getLspPolicy!()!;
			expect(secondPolicy.scope).not.toBe(policy.scope);
			await expect(secondPolicy.beforeExecute(execute)).rejects.toThrow("execution denied");
			await secondPolicy.beforeEdit(edit);
			await first.dispose();
			expect(shutdown).toHaveBeenCalledWith(policy.scope);
			await expect(policy.beforeEdit(edit)).rejects.toThrow("session is unavailable");
			await expect(policy.beforeExecute(execute)).rejects.toThrow("session is unavailable");
			expect(beforeEdit).toHaveBeenCalledTimes(1);
		} finally {
			close();
		}
	});

	it("a subagent session does not turn shared LSP off for its parent", async () => {
		using tempDir = TempDir.createSync("@pi-shared-lsp-binding-");
		const { start, close } = await starter(tempDir);
		const spy = vi.spyOn(lspClient, "setSharedLspEnabled");
		try {
			const parent = Settings.isolated({ "compaction.enabled": false, "lsp.shared": true });
			await start(parent, { enableLsp: true });
			expect(spy).toHaveBeenLastCalledWith(true);

			await start(createSubagentSettings(parent), {
				parentTaskPrefix: "0-Sub",
				taskDepth: 1,
				agentId: "0-Sub",
				enableLsp: false,
			});
			expect(spy).not.toHaveBeenCalledWith(false);

			await start(await parent.cloneForCwd(tempDir.path()), { bindProcessState: false, enableLsp: false });
			expect(spy).not.toHaveBeenCalledWith(false);

			// `Settings.isolated` values sit in the override layer, so flip that one.
			cfgLspShared.override(parent, false);
			// `listen` coalesces changes per microtask; let the queued one run.
			await Promise.resolve();
			expect(spy).toHaveBeenLastCalledWith(false);
		} finally {
			close();
		}
	});
});
