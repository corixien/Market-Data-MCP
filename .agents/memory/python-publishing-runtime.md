---
name: Python publishing runtime
description: Environment constraints for publishing Python apps in this workspace.
---

Use the full Python tools module when installing Python packages; the base Python module may not include pip and can trigger the externally-managed-environment guard. For Cloud Run deployment commands, use `python` rather than assuming `python3` exists.

**Why:** The base runtime did not expose pip, and the publish runtime could not resolve `python3`, even though the workspace workflow could run `python`.

**How to apply:** Keep Python dependencies in `requirements.txt`, use the package-management flow with a full Python tools module, and verify both the workspace workflow and the exact deployment command before publishing.

Remote MCP servers on Replit should expose Streamable HTTP at `/mcp`; the public proxy is the host boundary, so the SDK's localhost-only DNS-rebinding protection must be explicitly disabled or replaced with the deployed host allowlist.

**Why:** The MCP SDK defaults to accepting only localhost hosts, which rejects a published Replit hostname before MCP requests reach the server.

**How to apply:** Configure the transport security policy when mounting the Streamable HTTP app, and test the public `/mcp` URL after each republish.