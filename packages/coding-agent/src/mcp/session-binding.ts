import * as path from "node:path";
import type { Extension, LoadExtensionsResult } from "../extensibility/extensions/types";
import {
	MCPBootstrapError,
	type MCPSessionBinding,
	type MCPSessionBootstrap,
	type MCPSessionBootstrapInstallation,
} from "./bootstrap";
import { untilAborted, withTimeout } from "@oh-my-pi/pi-utils";
import { MCPManager } from "./manager";
import type { MCPDiscoverOptions, MCPLoadResult } from "./manager";
import type { MCPBeforeAuthEvent, MCPManagerAdmission } from "./types";
export type { MCPSessionBinding } from "./bootstrap";

type BindingState = {
	controller: AbortController;
	cleanup: () => Promise<void>;
	completion: PromiseWithResolvers<void>;
	started: boolean;
};
let bindings: WeakMap<object, BindingState> | undefined;
let managers: WeakMap<object, MCPSessionBinding> | undefined;

export function createMCPSessionBinding(cleanup: () => Promise<void> = async () => {}): MCPSessionBinding {
	const controller = new AbortController();
	const completion = Promise.withResolvers<void>();
	// A rejected cleanup remains observable without creating an unhandled rejection
	// merely because a consumer only watches the synchronous signal.
	void completion.promise.catch(() => {});
	const binding = Object.freeze({
		signal: controller.signal,
		revokePromise: completion.promise,
		revoke: () => revokeMCPSessionBinding(binding),
	}) as MCPSessionBinding;
	(bindings ??= new WeakMap()).set(binding, { controller, completion, cleanup, started: false });
	return binding;
}

export function isMCPSessionBinding(value: unknown): value is MCPSessionBinding {
	return typeof value === "object" && value !== null && bindings?.has(value) === true;
}
export function revokeMCPSessionBinding(binding: MCPSessionBinding, reason?: unknown): Promise<void> {
	const state = bindings?.get(binding);
	if (!state) throw new MCPBootstrapError("conflict", "Unknown native MCP binding");
	if (!state.started) {
		state.started = true;
		state.controller.abort(reason);
		void withTimeout(
			Promise.resolve().then(state.cleanup),
			3_000,
			new MCPBootstrapError("cleanup", "Native MCP generation cleanup did not drain"),
		).then(state.completion.resolve, error =>
			state.completion.reject(
				new MCPBootstrapError("cleanup", "Native MCP generation cleanup failed", { cause: error }),
			),
		);
	}
	return state.completion.promise;
}
export function bindMCPManager(manager: object, binding: MCPSessionBinding): void {
	if (!isMCPSessionBinding(binding) || binding.signal.aborted || managers?.has(manager))
		throw new MCPBootstrapError("conflict", "MCP manager already owned or binding unavailable");
	(managers ??= new WeakMap()).set(manager, binding);
}
export function getMCPManagerBinding(manager: unknown): MCPSessionBinding | undefined {
	return typeof manager === "object" && manager !== null ? managers?.get(manager) : undefined;
}

type Registration = {
	readonly source: string;
	open: boolean;
	callback?: MCPSessionBootstrap;
	error?: MCPBootstrapError;
	activeBinding?: MCPSessionBinding;
	claimed: boolean;
};
let registrations: WeakMap<Extension, Registration> | undefined;
export function registerMCPBootstrap(
	extension: Extension,
	source: string,
	callback: MCPSessionBootstrap,
	factoryOpen: boolean,
): void {
	let state = registrations?.get(extension);
	if (!state) {
		state = { source, open: factoryOpen, claimed: false };
		(registrations ??= new WeakMap()).set(extension, state);
	}
	const code = !state.open ? "late" : state.callback ? "duplicate" : undefined;
	if (code) {
		state.error = new MCPBootstrapError(code, "MCP bootstrap registration transaction rejected");
		void state.activeBinding?.revoke().catch(() => {});
		throw state.error;
	}
	if (typeof callback !== "function") {
		state.error = new MCPBootstrapError("initialization", "MCP bootstrap must be a function");
		throw state.error;
	}
	state.callback = callback;
}
export function closeMCPBootstrapRegistration(extension: Extension, succeeded: boolean): void {
	const state = registrations?.get(extension);
	if (!state) return;
	state.open = false;
	if (!succeeded) state.callback = undefined;
	if (state.error) throw state.error;
}
export function selectTrustedMCPBootstrap(
	result: LoadExtensionsResult,
	trustedPaths: readonly string[],
): { readonly extensionPath: string } | undefined {
	const eligible = result.extensions.filter(extension => registrations?.get(extension)?.callback);
	for (const extension of eligible) {
		const source = registrations?.get(extension)?.source;
		if (!source || !trustedPaths.includes(source))
			throw new MCPBootstrapError("source", "Bootstrap declaration is outside the trusted extension lane");
	}
	if (eligible.length > 1) throw new MCPBootstrapError("duplicate", "Multiple trusted MCP bootstrap sources");
	const source = eligible[0] && registrations?.get(eligible[0])?.source;
	return source ? { extensionPath: source } : undefined;
}
export function claimMCPBootstrap(
	result: LoadExtensionsResult,
	selection: { readonly extensionPath: string } | undefined,
): MCPSessionBootstrap | undefined {
	if (!registrations && !selection) return undefined;
	const declared = result.extensions.filter(extension => registrations?.get(extension)?.callback);
	if (!selection) {
		if (declared.length) throw new MCPBootstrapError("source", "MCP bootstrap requires native host selection");
		return undefined;
	}
	if (!path.isAbsolute(selection.extensionPath))
		throw new MCPBootstrapError("source", "MCP bootstrap source must be absolute and file-backed");
	const selected = result.extensions.find(
		extension => registrations?.get(extension)?.source === selection.extensionPath,
	);
	const state = selected && registrations?.get(selected);
	if (!state?.callback || state.open || state.error)
		throw state?.error ?? new MCPBootstrapError("source", "Selected MCP bootstrap source did not bind successfully");
	if (declared.length !== 1) throw new MCPBootstrapError("duplicate", "Multiple MCP bootstrap declarations");
	if (state.claimed)
		throw new MCPBootstrapError("conflict", "MCP bootstrap registration already claimed by another session");
	state.claimed = true;
	const callback = state.callback;
	return context => {
		if (state.error) throw state.error;
		state.activeBinding = context.binding;
		return callback(context);
	};
}

export function snapshotMCPBootstrapInstallation(value: unknown): MCPSessionBootstrapInstallation {
	if (
		typeof value !== "object" ||
		value === null ||
		!("loadConfigs" in value) ||
		typeof value.loadConfigs !== "function" ||
		!("admission" in value) ||
		typeof value.admission !== "function" ||
		!("dispose" in value) ||
		typeof value.dispose !== "function"
	)
		throw new MCPBootstrapError("initialization", "Incomplete native MCP bootstrap installation");
	return Object.freeze({
		loadConfigs: value.loadConfigs,
		admission: value.admission,
		dispose: value.dispose,
	}) as MCPSessionBootstrapInstallation;
}
export function wrapMCPBootstrapAdmission(
	binding: MCPSessionBinding,
	admission: MCPManagerAdmission,
): MCPManagerAdmission {
	return (name, config) => {
		binding.signal.throwIfAborted();
		const selected = admission(name, config);
		const signal = AbortSignal.any([binding.signal, selected.signal]);
		return Object.freeze({
			...selected,
			signal,
			transportOptions: Object.freeze({
				...selected.transportOptions,
				signal: selected.transportOptions.signal
					? AbortSignal.any([signal, selected.transportOptions.signal])
					: signal,
			}),
			beforeAuth: async (event: MCPBeforeAuthEvent) => {
				signal.throwIfAborted();
				await selected.beforeAuth({ ...event, signal: AbortSignal.any([signal, event.signal]) });
				signal.throwIfAborted();
			},
		});
	};
}

/** Native lifecycle owner; never exported by the public MCP barrel. */
export interface NativeMCPLifecycle {
	readonly manager: MCPManager | undefined;
	revoke(): Promise<void>;
	rotate(isAlive?: () => boolean): Promise<AsyncDisposable>;
}

export class NativeMCPBootstrapLifecycle implements NativeMCPLifecycle {
	#binding?: MCPSessionBinding;
	#manager?: MCPManager;
	#quarantined?: unknown;
	#rotating = false;
	get manager(): MCPManager | undefined {
		return this.#manager;
	}
	constructor(
		readonly bootstrap: MCPSessionBootstrap,
		readonly context: () => { cwd: string; sessionId: string; agentKind: "main" | "subagent" },
		readonly discoverOptions: MCPDiscoverOptions,
		readonly publish: (manager: MCPManager | undefined, result?: MCPLoadResult) => Promise<void>,
	) {}

	async initialize(discover: boolean): Promise<MCPManager> {
		if (this.#quarantined) throw this.#quarantined;
		const context = Object.freeze(this.context());
		let installation: MCPSessionBootstrapInstallation | undefined;
		let manager: MCPManager | undefined;
		let callback: Promise<MCPSessionBootstrapInstallation> | undefined;
		let disposed = false;
		let returnedDispose: (() => Promise<void>) | undefined;
		const pendingLoads = new Set<Promise<unknown>>();
		const disposeInstallation = async () => {
			if (!returnedDispose || disposed) return;
			disposed = true;
			await returnedDispose();
		};
		const binding = createMCPSessionBinding(async () => {
			try {
				const withdrawal = this.publish(undefined);
				// A callback which ignores cancellation must not be reported drained.
				const release = (async () => {
					await callback?.catch(() => {});
					const resources = await Promise.allSettled([
						manager?.drainOwnedSession(),
						disposeInstallation(),
						Promise.allSettled(pendingLoads),
					]);
					const failures = resources.filter(result => result.status === "rejected");
					if (failures.length)
						throw new AggregateError(
							failures.map(result => result.reason),
							"Native MCP resources did not drain",
						);
				})();
				const results = await Promise.allSettled([withdrawal, release]);
				const failures = results.filter(result => result.status === "rejected");
				if (failures.length)
					throw new AggregateError(
						failures.map(result => result.reason),
						"Native MCP generation cleanup failed",
					);
			} catch (error) {
				this.#quarantined = error;
				throw error;
			}
		});
		this.#binding = binding;
		try {
			callback = Promise.resolve()
				.then(() => {
					binding.signal.throwIfAborted();
					return this.bootstrap(Object.freeze({ ...context, binding }));
				})
				.then(value => {
					const returned: unknown = value;
					if (
						typeof returned === "object" &&
						returned !== null &&
						"dispose" in returned &&
						typeof returned.dispose === "function"
					) {
						const dispose = returned.dispose;
						returnedDispose = async () => {
							await dispose();
						};
					}
					installation = snapshotMCPBootstrapInstallation(value);
					return installation;
				});
			installation = await untilAborted(binding.signal, callback);
			binding.signal.throwIfAborted();
			const loadConfigs = installation.loadConfigs;
			manager = new MCPManager(
				context.cwd,
				null,
				async (cwd, options) => {
					binding.signal.throwIfAborted();
					const loading = Promise.resolve().then(() => {
						binding.signal.throwIfAborted();
						return loadConfigs(cwd, options);
					});
					pendingLoads.add(loading);
					void loading.then(
						() => pendingLoads.delete(loading),
						() => pendingLoads.delete(loading),
					);
					const result = await untilAborted(binding.signal, loading);
					binding.signal.throwIfAborted();
					return result;
				},
				undefined,
				wrapMCPBootstrapAdmission(binding, installation.admission),
			);
			bindMCPManager(manager, binding);
			this.#manager = manager;
			await this.publish(manager);
			binding.signal.throwIfAborted();
			if (discover) await this.discover();
			binding.signal.throwIfAborted();
			return manager;
		} catch (error) {
			void this.revoke().catch(() => {});
			throw new MCPBootstrapError("initialization", "Native MCP bootstrap initialization failed", { cause: error });
		}
	}

	async discover(): Promise<void> {
		const binding = this.#binding;
		const manager = this.#manager;
		if (!binding || !manager) throw new MCPBootstrapError("initialization", "Native MCP generation is unavailable");
		binding.signal.throwIfAborted();
		try {
			const result = await manager.discoverAndConnect(this.discoverOptions);
			binding.signal.throwIfAborted();
			if (result.errors.size) throw new MCPBootstrapError("initialization", "Guarded MCP discovery failed");
			await this.publish(manager, result);
			binding.signal.throwIfAborted();
		} catch (error) {
			await this.revoke();
			throw new MCPBootstrapError("initialization", "Guarded MCP discovery failed", { cause: error });
		}
	}

	revoke(): Promise<void> {
		return this.#binding?.revoke() ?? Promise.resolve();
	}

	async rotate(isAlive?: () => boolean): Promise<AsyncDisposable> {
		if (this.#rotating) throw new MCPBootstrapError("conflict", "Another native MCP context transition is active");
		this.#rotating = true;
		try {
			// revoke() aborts synchronously, before this method's first await.
			await this.revoke();
		} catch (error) {
			this.#rotating = false;
			throw error;
		}
		return {
			[Symbol.asyncDispose]: async () => {
				try {
					if (isAlive?.() !== false) await this.initialize(true);
				} finally {
					this.#rotating = false;
				}
			},
		};
	}
}
