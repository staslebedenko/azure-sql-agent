"""
Utility — convert Python tool functions to OpenAI tool schemas and
dispatch tool calls during an Assistants API run.

Replaces the ``FunctionTool`` / ``ToolSet`` / ``create_thread_and_process_run``
convenience from ``azure-ai-agents`` / ``azure-ai-projects`` SDKs.
Works directly with ``openai.AzureOpenAI`` Assistants API.
"""

from __future__ import annotations

import inspect
import json
import time
from typing import Any, Callable


# ─────────────────────────────────────────────────────────────────────────────
# Schema generation
# ─────────────────────────────────────────────────────────────────────────────

_TYPE_MAP = {
    int: "integer",
    float: "number",
    str: "string",
    bool: "boolean",
    # Also handle stringified annotations from `from __future__ import annotations`
    "int": "integer",
    "float": "number",
    "str": "string",
    "bool": "boolean",
}

_CAST_MAP = {
    int: int,
    float: float,
    "int": int,
    "float": float,
}


def func_to_tool_schema(func: Callable) -> dict:
    """Convert a Python function to an OpenAI function-tool schema.

    Extracts parameter types from annotations and descriptions from the
    ``Args:`` section of the docstring.
    """
    sig = inspect.signature(func)
    doc = (func.__doc__ or "").strip()
    description = doc.split("\n")[0] if doc else func.__name__

    # Parse "Args:" block for per-parameter descriptions
    arg_descs: dict[str, str] = {}
    if "Args:" in doc:
        args_section = doc.split("Args:")[1]
        if "Returns:" in args_section:
            args_section = args_section.split("Returns:")[0]
        for line in args_section.strip().split("\n"):
            line = line.strip()
            if ":" in line:
                pname, pdesc = line.split(":", 1)
                arg_descs[pname.strip()] = pdesc.strip()

    properties: dict[str, Any] = {}
    required: list[str] = []

    for name, param in sig.parameters.items():
        ptype = _TYPE_MAP.get(param.annotation, "string")
        prop: dict[str, Any] = {"type": ptype}
        if name in arg_descs:
            prop["description"] = arg_descs[name]
        properties[name] = prop

        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema: dict[str, Any] = {
        "type": "function",
        "function": {
            "name": func.__name__,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
            },
        },
    }
    if required:
        schema["function"]["parameters"]["required"] = required

    return schema


# ─────────────────────────────────────────────────────────────────────────────
# Run loop with automatic function dispatch
# ─────────────────────────────────────────────────────────────────────────────

def run_assistant_with_tools(
    client,  # openai.AzureOpenAI
    assistant_id: str,
    thread_id: str,
    tool_map: dict[str, Callable],
    *,
    max_iterations: int = 15,
) -> str:
    """Create a run, poll until complete, dispatch any tool calls.

    Returns the assistant's final text response.
    """
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)

        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread_id,
            assistant_id=assistant_id,
        )

        for _ in range(max_iterations):
            if run.status == "completed":
                break

            if run.status == "requires_action":
                tool_calls = run.required_action.submit_tool_outputs.tool_calls
                outputs: list[dict] = []
                for tc in tool_calls:
                    func = tool_map.get(tc.function.name)
                    if func is None:
                        result = json.dumps(
                            {"error": f"Unknown function: {tc.function.name}"}
                        )
                    else:
                        try:
                            kwargs = json.loads(tc.function.arguments)
                            # Cast numeric types that arrive as strings
                            sig = inspect.signature(func)
                            for pname, pparam in sig.parameters.items():
                                cast_fn = _CAST_MAP.get(pparam.annotation)
                                if pname in kwargs and cast_fn is not None:
                                    kwargs[pname] = cast_fn(kwargs[pname])
                            print(f"    🔧 {tc.function.name}({kwargs})")
                            result = func(**kwargs)
                        except Exception as exc:
                            result = json.dumps({"error": str(exc)})
                    outputs.append({"tool_call_id": tc.id, "output": result})

                run = client.beta.threads.runs.submit_tool_outputs_and_poll(
                    thread_id=thread_id,
                    run_id=run.id,
                    tool_outputs=outputs,
                )
                continue

            if run.status in ("failed", "cancelled", "expired"):
                err = getattr(run, "last_error", None)
                return f"(Run {run.status}: {err})"

            # Still in progress — wait a bit
            time.sleep(1)

        # Retrieve final assistant message
        messages = client.beta.threads.messages.list(thread_id=thread_id)
        for msg in messages.data:
            if msg.role == "assistant" and msg.content:
                return msg.content[0].text.value

    return "(No response from agent)"
