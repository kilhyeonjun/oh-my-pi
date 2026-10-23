import type { LinterClient, LspPolicy, ServerConfig } from "../../lsp/types";
import { LspLinterClient } from "./lsp-linter-client";

/**
 * Linter client implementations.
 *
 * The LinterClient interface provides a common API for formatters and linters.
 * Different implementations can use LSP protocol, CLI tools, or other mechanisms.
 */

export { BiomeClient } from "./biome-client";
export { LspLinterClient } from "./lsp-linter-client";
export { SwiftLintClient } from "./swiftlint-client";

// Cache of linter clients by server name + cwd
const clientCache = new Map<string, LinterClient>();

/**
 * Get or create a linter client for a server configuration.
 * Uses the server's custom factory if provided, otherwise falls back to LSP.
 */
export function getLinterClient(
	serverName: string,
	config: ServerConfig,
	cwd: string,
	policy?: LspPolicy,
): LinterClient {
	const key = `${serverName}:${cwd}${policy ? `\0${policy.scope}` : ""}`;

	let client = clientCache.get(key);
	if (client) {
		return client;
	}

	// Use custom factory if provided
	if (config.createClient) {
		client = config.createClient(config, cwd, policy);
	} else {
		// Default to LSP
		client = LspLinterClient.create(config, cwd, policy);
	}

	clientCache.set(key, client);
	return client;
}

/** Release only the linter clients retaining a disposed session's policy. */
export function clearScopedLinterClients(scope: string): void {
	for (const [key, client] of clientCache) {
		if (!key.endsWith(`\0${scope}`)) continue;
		clientCache.delete(key);
		client.dispose?.();
	}
}

/**
 * Clear all cached linter clients.
 */
export function clearLinterClientCache(): void {
	for (const client of clientCache.values()) {
		client.dispose?.();
	}
	clientCache.clear();
}
