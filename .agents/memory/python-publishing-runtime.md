---
name: Python publishing runtime
description: Environment constraints for publishing Python apps in this workspace.
---

Use the full Python tools module when installing Python packages; the base Python module may not include pip and can trigger the externally-managed-environment guard. For Cloud Run deployment commands, use `python` rather than assuming `python3` exists.

**Why:** The base runtime did not expose pip, and the publish runtime could not resolve `python3`, even though the workspace workflow could run `python`.

**How to apply:** Keep Python dependencies in `requirements.txt`, use the package-management flow with a full Python tools module, and verify both the workspace workflow and the exact deployment command before publishing.