import { afterEach, describe, expect, it } from "bun:test";
import * as path from "node:path";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import { configCache } from "@oh-my-pi/pi-coding-agent/lsp/config";
import { LspTool } from "@oh-my-pi/pi-coding-agent/lsp/tool";
import type { LspPolicy } from "@oh-my-pi/pi-coding-agent/lsp/types";
import type { ToolSession } from "@oh-my-pi/pi-coding-agent/tools";
import { ToolError } from "@oh-my-pi/pi-tui/tools/tool-errors";
import { TempDir } from "@oh-my-pi/pi-utils";

const roots: TempDir[] = [];
afterEach(() => {
	for (const root of roots.splice(0)) {
		configCache.delete(root.path());
		root.removeSync();
	}
});

async function fixture(beforeEdit: LspPolicy["beforeEdit"]) {
	const root = TempDir.createSync("@omp-lsp-tool-policy-");
	roots.push(root);
	configCache.set(root.path(), { servers: {} });
	const source = path.join(root.path(), "old.ts");
	const destination = path.join(root.path(), "new.ts");
	await Bun.write(source, "export const value = 1;\n");
	const policy: LspPolicy = { scope: root.path(), beforeExecute: async () => {}, beforeEdit };
	const session = { cwd: root.path(), settings: Settings.isolated(), getLspPolicy: () => policy } as ToolSession;
	return { root, source, destination, tool: new LspTool(session) };
}

describe("LSP file rename policy", () => {
	it("denies before moving the source and supplies absolute change paths", async () => {
		const requests: Parameters<LspPolicy["beforeEdit"]>[0][] = [];
		const { root, source, destination, tool } = await fixture(async request => {
			requests.push(request);
			throw new ToolError("rename denied");
		});
		await expect(
			tool.execute("deny", { action: "rename_file", file: source, new_name: destination }),
		).rejects.toThrow("rename denied");
		expect(requests).toEqual([
			{
				origin: "file_rename",
				cwd: root.path(),
				changes: [{ kind: "rename", oldPath: source, newPath: destination, overwrite: false }],
			},
		]);
		expect(await Bun.file(source).text()).toBe("export const value = 1;\n");
		expect(await Bun.file(destination).exists()).toBe(false);
	});

	it("allows the existing atomic rename implementation after admission", async () => {
		let gates = 0;
		const { source, destination, tool } = await fixture(async () => {
			gates++;
		});
		const result = await tool.execute("allow", { action: "rename_file", file: source, new_name: destination });
		expect(gates).toBe(1);
		expect(result.details?.success).toBe(true);
		expect(await Bun.file(source).exists()).toBe(false);
		expect(await Bun.file(destination).text()).toBe("export const value = 1;\n");
	});

	it("does not request mutation authority for preview", async () => {
		let gates = 0;
		const { source, destination, tool } = await fixture(async () => {
			gates++;
			throw new ToolError("denied");
		});
		await tool.execute("preview", { action: "rename_file", file: source, new_name: destination, apply: false });
		expect(gates).toBe(0);
		expect(await Bun.file(source).exists()).toBe(true);
		expect(await Bun.file(destination).exists()).toBe(false);
	});
});
