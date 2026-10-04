"""FinQA 小范围离线数值 oracle；不是模型，也不是完整官方 evaluator。

只解释白名单二元算术及向后 #n 引用，不使用 eval。表格聚合、比较、
任意代码、非有限数值均 fail closed，不能用抄写 gold answer 代替计算。
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, DecimalException, localcontext
from typing import Any, Mapping


NUMBER = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)"
STEP = re.compile(r"([a-z_]+)\(\s*([^(),]+?)\s*,\s*([^(),]+?)\s*\)")
OPS = {"add", "subtract", "multiply", "divide", "exp"}


def number(value: Any) -> Decimal:
    """解析常见 FinQA 数字；百分号按 program 的字面数值处理。"""
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("expected a finite numeric value")
    text = str(value).strip().replace(",", "").replace("$", "").strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    if len(text) > 100 or not re.fullmatch(NUMBER, text):
        raise ValueError(f"unsupported numeric value: {value!r}")
    result = Decimal(text)
    if not result.is_finite() or abs(result) > Decimal("1e100"):
        raise ValueError("numeric value exceeds replay bound")
    return result


def execute_program(program: str) -> dict[str, Any]:
    """执行 <=32 步程序，保留每步操作数/结果供 artifact 审计。"""
    if not isinstance(program, str) or not program.strip() or len(program) > 10000:
        raise ValueError("program must be nonempty text within 10000 characters")
    values: list[Decimal] = []
    trace = []
    remaining = program.strip()

    def operand(token: str) -> Decimal:
        if token.startswith("#"):
            if not re.fullmatch(r"#\d+", token) or int(token[1:]) >= len(values):
                raise ValueError(f"invalid/forward program reference: {token}")
            return values[int(token[1:])]
        if token.startswith("const_"):
            token = token[6:]
            token = "-1" if token == "m1" else token.replace("_", ".")
        return number(token)

    with localcontext() as ctx:
        ctx.prec = 40
        while remaining:
            match = STEP.match(remaining)
            if match is None or len(values) >= 32:
                raise ValueError("unsupported program syntax or step limit exceeded")
            op, left, right = match.groups()
            if op not in OPS:
                raise ValueError(f"unsupported replay operation: {op}")
            a, b = operand(left.strip()), operand(right.strip())
            try:
                if op == "add":
                    value = a + b
                elif op == "subtract":
                    value = a - b
                elif op == "multiply":
                    value = a * b
                elif op == "divide":
                    value = a / b
                else:
                    if b != b.to_integral_value() or abs(b) > 10:
                        raise ValueError("exp requires an integer exponent in [-10, 10]")
                    value = a ** int(b)
            except DecimalException as exc:
                raise ValueError(f"invalid arithmetic at step {len(values)}") from exc
            if not value.is_finite() or abs(value) > Decimal("1e100"):
                raise ValueError("program result exceeds replay bound")
            trace.append({"operation": op, "operands": [str(a), str(b)], "result": str(value)})
            values.append(value)
            remaining = remaining[match.end():].strip()
            if remaining:
                if not remaining.startswith(",") or not remaining[1:].strip():
                    raise ValueError("program steps require a comma separator")
                remaining = remaining[1:].strip()
    return {"program": program, "value": str(values[-1]), "trace": trace}


def question_prompt(record: Mapping[str, Any]) -> str:
    """构造只包含问题与可执行 program 的提示。

    刻意不放入 ``answer``/``exe_ans``：适配器必须自己算，而不是抄写 gold 字段。
    Gate B/Gate C 与烟测脚本共用这一份构造逻辑，避免提示漂移。
    """
    qa = record["qa"]
    return (
        "Solve this FinQA arithmetic question. Return only a JSON object "
        'with one string field named "answer"; do not include explanation.\n'
        f"question: {qa['question']}\n"
        f"program: {qa['program']}\n"
        f"table: {json.dumps(record.get('table', []), ensure_ascii=False)}"
    )


def compare_answer(computed: str | None, answer: Any) -> dict[str, Any]:
    """比较 FinQA 展示答案；百分号同时支持 ratio 和 percentage program 输出。"""
    try:
        actual = number(computed)
        raw = number(answer)
        candidates = [(raw, "direct")]
        if str(answer).strip().endswith("%"):
            candidates.append((raw / Decimal(100), "ratio"))
        expected, mode = min(candidates, key=lambda item: abs(actual - item[0]))
        # gold 的最后一位允许半个单位的舍入误差；不是任意相对容差。
        tolerance = Decimal(5).scaleb(expected.as_tuple().exponent - 1)
        return {
            "ok": abs(actual - expected) <= tolerance,
            "normalized_gold": str(expected),
            "tolerance": str(tolerance),
            "percent_mode": mode,
        }
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}


def check_oracle(computed: str, qa: Mapping[str, Any]) -> dict[str, Any]:
    """exe_ans 和展示 answer 分开校验；exe_ans 缺失时不冒充完整 oracle。"""
    answer_check = compare_answer(computed, qa.get("answer"))
    if "exe_ans" not in qa:
        return {"ok": False, "answer": answer_check, "error": "qa.exe_ans is required for numeric oracle"}
    actual, expected = number(computed), number(qa["exe_ans"])
    # 官方数值通常保留 5 位小数；明确记录容差，不把 answer 自复制当作验证。
    tolerance = Decimal("0.000005")
    executable_ok = abs(actual - expected) <= tolerance
    return {"ok": executable_ok and answer_check["ok"], "exe_ans_ok": executable_ok,
            "expected_exe_ans": str(expected), "exe_ans_tolerance": str(tolerance), "answer": answer_check}
