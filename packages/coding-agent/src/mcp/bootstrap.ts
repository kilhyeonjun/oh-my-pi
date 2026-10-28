import type { MCPConfigLoader } from "./manager";
import type { MCPManagerAdmission } from "./types";

declare const mcpSessionBindingBrand: unique symbol;

/** A native session/source generation. Revocation is synchronous; cleanup is awaited. */
export interface MCPSessionBinding {
	readonly [mcpSessionBindingBrand]: true;
	readonly signal: AbortSignal;
	readonly revokePromise: Promise<void>;
	revoke(): Promise<void>;
}

export interface MCPSessionBootstrapContext {
	readonly binding: MCPSessionBinding;
	readonly cwd: string;
	readonly sessionId: string;
	readonly agentKind: "main" | "subagent";
}

export interface MCPSessionBootstrapInstallation {
	readonly loadConfigs: MCPConfigLoader;
	readonly admission: MCPManagerAdmission;
	readonly dispose: () => void | Promise<void>;
}

export type MCPSessionBootstrap = (
	context: MCPSessionBootstrapContext,
) => MCPSessionBootstrapInstallation | Promise<MCPSessionBootstrapInstallation>;

export class MCPBootstrapError extends Error {
	constructor(
		readonly code: "source" | "duplicate" | "late" | "conflict" | "initialization" | "cleanup",
		message: string,
		options?: ErrorOptions,
	) {
		super(message, options);
		this.name = "MCPBootstrapError";
	}
}
