import { afterEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { removeWithRetries, withTimeout } from "@oh-my-pi/pi-utils";
import { runMCPAdmission } from "../src/mcp/admission";
import { connectToServer, MCPConnectionTimeoutError } from "../src/mcp/client";
import { MCPAdmissionError } from "../src/mcp/errors";
import { createStdioTransport } from "../src/mcp/transports/stdio";
import type { MCPStdioServerConfig, MCPTransport, MCPTransportOptions } from "../src/mcp/types";

interface ProcessFixture {
	dir: string;
	config: MCPStdioServerConfig;
	env: Record<string, string>;
	frames(): Promise<Record<string, unknown>[]>;
	started(): Promise<boolean>;
}

const roots: string[] = [];
const transports: MCPTransport[] = [];
afterEach(async () => {
	await Promise.allSettled(transports.splice(0).map(transport => bounded(transport.close())));
	for (const root of roots.splice(0)) await removeWithRetries(root);
});

function bounded<T>(promise: Promise<T>): Promise<T> {
	return withTimeout(promise, 2_000, "stdio admission or cleanup did not settle");
}
const turn = () => new Promise<void>(resolve => setImmediate(resolve));

// Real native child/pipe effects, with only disposable files and synthetic values.
const server = `
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const dir = process.env.MCP_FIXTURE_DIR;
fs.writeFileSync(path.join(dir, 'started'), String(process.pid));
const frames = path.join(dir, 'frames');
fs.writeFileSync(frames, '');
const send = value => process.stdout.write(JSON.stringify(value) + '\\n');
let initializing;
const initialized = id => send({jsonrpc:'2.0', id, result:{protocolVersion:'2024-11-05', capabilities:{tools:{}}, serverInfo:{name:'fixture',version:'1'}}});
readline.createInterface({input:process.stdin}).on('line', line => {
 fs.appendFileSync(frames, line + '\\n');
 const message = JSON.parse(line);
 if (message.method === 'notifications/initialized') { send({jsonrpc:'2.0',method:'fixture/ready'}); return; }
 if (message.method === 'fixture/ask') { send({jsonrpc:'2.0',id:'server-request',method:'ping'}); return; }
 if (message.method === 'fixture/guess') { send({jsonrpc:'2.0',id:message.params.requestId,result:{premature:true}}); return; }
 if (!message.method) { if (initializing !== undefined) initialized(initializing); return; }
 if (message.id === undefined) return;
 if (message.method === 'initialize') {
  if (process.env.MCP_PING_HANDSHAKE === 'yes') { initializing=message.id; send({jsonrpc:'2.0',id:'server-request',method:'ping'}); }
  else initialized(message.id);
 } else send({jsonrpc:'2.0',id:message.id,result:{ambient:process.env.MCP_ADMISSION_TEST_LEAK !== undefined,configured:process.env.MCP_UNAPPROVED_CONFIG !== undefined,approved:process.env.MCP_APPROVED === 'fixture'}});
});
`;

async function fixture(extraEnv: Record<string, string> = {}): Promise<ProcessFixture> {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mcp-admission-process-"));
	roots.push(dir);
	const config: MCPStdioServerConfig = {
		type: "stdio",
		command: process.execPath,
		args: ["-e", server],
		cwd: dir,
		timeout: 100,
	};
	const env = { HOME: dir, PATH: "/usr/bin:/bin", MCP_FIXTURE_DIR: dir, MCP_APPROVED: "fixture", ...extraEnv };
	return {
		dir,
		config,
		env,
		async frames(): Promise<Record<string, unknown>[]> {
			const content = await fs.readFile(path.join(dir, "frames"), "utf8").catch((error: NodeJS.ErrnoException) => {
				if (error.code === "ENOENT") return "";
				throw error;
			});
			return content.trim()
				? content
						.trim()
						.split("\n")
						.map(line => JSON.parse(line))
				: [];
		},
		async started() {
			try {
				await fs.access(path.join(dir, "started"));
				return true;
			} catch (error) {
				if (error instanceof Error && "code" in error && error.code === "ENOENT") return false;
				throw error;
			}
		},
	};
}

async function connect(local: ProcessFixture, options: MCPTransportOptions) {
	const transport = await createStdioTransport(local.config, { ...options, stdioEnv: local.env });
	transports.push(transport);
	return transport;
}

describe("guarded MCP native process effects", () => {
	it("denied spawn creates no child and discards callback diagnostics", async () => {
		const local = await fixture();
		const spawn = spyOn(Bun, "spawn"); // Transparent observer; native behavior is not replaced.
		try {
			const failure = bounded(
				createStdioTransport(local.config, {
					stdioEnv: local.env,
					beforeSpawn() {
						throw new Error("private spawn diagnostic");
					},
				}),
			);
			await expect(failure).rejects.toBeInstanceOf(MCPAdmissionError);
			await expect(failure).rejects.not.toThrow("private spawn diagnostic");
			expect(spawn.mock.calls).toHaveLength(0);
			expect(await local.started()).toBe(false);
		} finally {
			spawn.mockRestore();
		}
	});

	it.skipIf(process.platform === "win32")(
		"exact child PATH selects a bare fixture command without ambient or config environment",
		async () => {
			const local = await fixture();
			const command = "mcp-admission-fixture-bun";
			await fs.symlink(process.execPath, path.join(local.dir, command));
			local.config.command = command;
			local.config.env = { MCP_UNAPPROVED_CONFIG: "synthetic", PATH: "/nonexistent" };
			local.env.PATH = local.dir;
			const old = process.env.MCP_ADMISSION_TEST_LEAK;
			process.env.MCP_ADMISSION_TEST_LEAK = "synthetic";
			try {
				const transport = await connect(local, { beforeSpawn() {}, beforeSend() {} });
				await expect(bounded(transport.request("fixture/env"))).resolves.toEqual({
					ambient: false,
					configured: false,
					approved: true,
				});
			} finally {
				if (old === undefined) delete process.env.MCP_ADMISSION_TEST_LEAK;
				else process.env.MCP_ADMISSION_TEST_LEAK = old;
			}
			const missing = await fixture();
			missing.config.command = command;
			missing.config.env = { PATH: local.dir };
			await expect(
				bounded(createStdioTransport(missing.config, { stdioEnv: missing.env, beforeSpawn() {} })),
			).rejects.toMatchObject({ code: "ENOENT" });
			expect(await missing.started()).toBe(false);
		},
	);

	it("a never-settling request gate honors the request deadline and a late allow writes nothing", async () => {
		const local = await fixture();
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const transport = await connect(local, {
			beforeSend() {
				entered.resolve();
				return allow.promise;
			},
		});
		const request = transport.request("tools/call", { name: "unapproved" });
		await bounded(entered.promise);
		await expect(bounded(request)).rejects.toMatchObject({ failure: "timeout" });
		allow.resolve();
		await turn();
		await bounded(transport.close());
		expect(await local.frames()).toEqual([]);
	});

	it("explicit abort with disabled deadline retires the send and observes a late rejection", async () => {
		const local = await fixture();
		local.config.timeout = 0;
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const transport = await connect(local, {
			beforeSend() {
				entered.resolve();
				return allow.promise;
			},
		});
		const controller = new AbortController();
		const request = transport.request("tools/call", {}, { signal: controller.signal });
		await bounded(entered.promise);
		controller.abort(new DOMException("caller canceled", "AbortError"));
		await expect(bounded(request)).rejects.toMatchObject({ name: "AbortError" });
		allow.reject(new Error("private late diagnostic"));
		await turn();
		await bounded(transport.close());
		expect(await local.frames()).toEqual([]);
	});

	it("close bounds a never-settling notification but does not attest callback drain", async () => {
		const local = await fixture();
		local.config.timeout = 0;
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const transport = await connect(local, {
			beforeSend() {
				entered.resolve();
				return allow.promise;
			},
		});
		const notification = transport.notify("notifications/initialized");
		// Observe before close synchronously aborts its local send scope.
		const observed = notification.then(
			() => undefined,
			(error: unknown) => error,
		);
		await bounded(entered.promise);
		await expect(bounded(transport.close())).rejects.toMatchObject({ failure: "unknown", retryable: false });
		expect(await bounded(observed)).toMatchObject({ name: "AbortError" });
		allow.resolve();
		await turn();
		expect(transport.connected).toBe(false);
		expect(await local.frames()).toEqual([]);
	});

	it("server-response denial sends no compensating error and reveals no handler or policy diagnostic", async () => {
		const local = await fixture();
		const denied = Promise.withResolvers<Error>();
		const transport = await connect(local, {
			beforeSend(event) {
				const body = JSON.parse(event.body!);
				if (body.id === "server-request") {
					expect(event.body).not.toContain("private handler diagnostic");
					throw new Error("private policy diagnostic");
				}
			},
		});
		transport.onRequest = async () => {
			throw new Error("private handler diagnostic");
		};
		transport.onError = error => denied.resolve(error);
		await bounded(transport.notify("fixture/ask"));
		const error = await bounded(denied.promise);
		expect(error).toBeInstanceOf(MCPAdmissionError);
		expect(String(error)).not.toContain("private");
		await bounded(transport.close());
		expect((await local.frames()).map(frame => frame.method)).toEqual(["fixture/ask"]);
	});

	it("a guessed response cannot complete a request while its wire gate is pending", async () => {
		const local = await fixture();
		const allow = Promise.withResolvers<void>();
		const entered = Promise.withResolvers<string | number>();
		const transport = await connect(local, {
			beforeSend(event) {
				const message = JSON.parse(event.body!);
				if (message.method === "tools/call") {
					entered.resolve(message.id);
					return allow.promise;
				}
			},
		});
		const request = transport.request("tools/call");
		const requestId = await bounded(entered.promise);
		await bounded(transport.notify("fixture/guess", { requestId }));
		await expect(bounded(request)).rejects.toMatchObject({ failure: "timeout" });
		allow.resolve();
		await turn();
		await bounded(transport.close());
		expect((await local.frames()).map(frame => frame.method)).toEqual(["fixture/guess"]);
	});

	it("the real client handshake completes only after an admitted server-to-client ping response", async () => {
		const local = await fixture({ MCP_PING_HANDSHAKE: "yes" });
		local.config.timeout = 1_000;
		const ready = Promise.withResolvers<void>();
		const connection = await bounded(
			connectToServer("fixture", local.config, {
				transportOptions: { stdioEnv: local.env, beforeSpawn() {}, beforeSend() {} },
				onNotification(method) {
					if (method === "fixture/ready") ready.resolve();
				},
			}),
		);
		transports.push(connection.transport);
		await bounded(ready.promise);
		expect(connection.serverInfo).toEqual({ name: "fixture", version: "1" });
		await bounded(connection.transport.close());
		const frames = await local.frames();
		expect(frames.map(frame => frame.method ?? frame.id)).toEqual([
			"initialize",
			"server-request",
			"notifications/initialized",
		]);
	});

	it("client deadline retires a pending factory and late approval cannot invoke native spawn", async () => {
		const local = await fixture();
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const spawn = spyOn(Bun, "spawn");
		try {
			const connection = connectToServer("fixture", local.config, {
				transportOptions: {
					stdioEnv: local.env,
					beforeSpawn() {
						entered.resolve();
						return allow.promise;
					},
				},
			});
			await bounded(entered.promise);
			await expect(bounded(connection)).rejects.toBeInstanceOf(MCPConnectionTimeoutError);
			allow.resolve();
			await turn();
			expect(spawn.mock.calls).toHaveLength(0);
			expect(await local.started()).toBe(false);
		} finally {
			spawn.mockRestore();
		}
	});

	it("owner-triggered stdio teardown preserves terminal denial before the request settles", async () => {
		const local = await fixture();
		const owner = new AbortController();
		const denial = new MCPAdmissionError();
		const transport = await connect(local, {
			signal: owner.signal,
			beforeSend() {
				owner.abort(denial);
				throw new Error("private callback diagnostic");
			},
		});
		await expect(bounded(transport.request("tools/call"))).rejects.toBe(denial);
		await bounded(transport.close());
		expect(await local.frames()).toEqual([]);
	});

	it("manager retirement preserves native terminal denial rather than an abort-shaped retry", async () => {
		const owner = new AbortController();
		const denial = new MCPAdmissionError();
		await expect(
			bounded(
				runMCPAdmission(owner.signal, () => {
					owner.abort(denial);
					throw new Error("private callback");
				}),
			),
		).rejects.toBe(denial);
	});
});
