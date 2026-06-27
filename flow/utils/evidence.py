"""
证据链模块 — 贯穿整个流水线的溯源机制

每条 claim 必须包含:
  - source: 引用来源（文字稿段落/视频时间戳）
  - confidence: 置信度 0.0-1.0
  - verifier_results: 校验结果列表
  - trace: 完整的 agent 处理链路
"""

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path


def hash_content(content: str) -> str:
    """对内容生成短哈希，用于追踪数据流转"""
    return hashlib.sha256(content.encode()).hexdigest()[:12]


class EvidenceChain:
    """证据链记录器——贯穿整个流水线"""

    def __init__(self, date_str: str):
        self.date_str = date_str
        self.pipeline_id = f"{date_str}_finance_{int(time.time())}"
        self.traces: list[dict] = []
        self.claims: list[dict] = []
        self.verification_rounds: list[dict] = []

    def record_agent(self, agent_name: str, input_data: dict,
                     output_data: dict, metadata: dict = None):
        """记录一个 Agent 的输入→输出流转"""
        trace = {
            "agent": agent_name,
            "timestamp": datetime.now().isoformat(),
            "input_hash": hash_content(json.dumps(input_data, ensure_ascii=False, sort_keys=True)),
            "output_hash": hash_content(json.dumps(output_data, ensure_ascii=False, sort_keys=True)),
            "metadata": metadata or {},
        }
        self.traces.append(trace)
        return trace

    def record_claim(self, claim_id: str, claim_text: str,
                     source: str, agent: str, confidence: float):
        """记录一条分析结论"""
        claim = {
            "claim_id": claim_id,
            "claim_text": claim_text,
            "source": source,  # 格式: "文字稿段落X" 或 "新闻标题+时间戳"
            "agent": agent,
            "confidence": confidence,
            "verification_results": [],
            "final_verdict": None,
        }
        self.claims.append(claim)
        return claim

    def record_verification(self, claim_id: str, verifier: str,
                            dimension: str, verdict: str,
                            correction: str = "", score: float = 0.0):
        """记录一次校验结果"""
        result = {
            "claim_id": claim_id,
            "verifier": verifier,
            "dimension": dimension,  # fact / compliance / logic
            "verdict": verdict,       # pass / fail / uncertain
            "correction": correction,
            "score": score,
            "timestamp": datetime.now().isoformat(),
        }
        # 找到对应的claim并追加校验结果
        for claim in self.claims:
            if claim["claim_id"] == claim_id:
                claim["verification_results"].append(result)
                break
        self.verification_rounds.append(result)
        return result

    def finalize_claim(self, claim_id: str, verdict: str):
        """标记一条claim的最终校验结论"""
        for claim in self.claims:
            if claim["claim_id"] == claim_id:
                claim["final_verdict"] = verdict
                break

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "date": self.date_str,
            "generated_at": datetime.now().isoformat(),
            "total_claims": len(self.claims),
            "passed_claims": sum(
                1 for c in self.claims if c["final_verdict"] == "pass"
            ),
            "failed_claims": sum(
                1 for c in self.claims if c["final_verdict"] == "fail"
            ),
            "uncertain_claims": sum(
                1 for c in self.claims
                if c["final_verdict"] not in ("pass", "fail")
            ),
            "verification_rounds": len(self.verification_rounds),
            "traces": self.traces,
            "claims": self.claims,
        }

    def save(self, output_dir: Path):
        """保存证据链到JSON文件"""
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "evidence_chain.json"
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path

    def print_summary(self):
        """打印证据链摘要"""
        d = self.to_dict()
        print(f"\n{'='*60}")
        print(f"  证据链摘要")
        print(f"{'='*60}")
        print(f"  Pipeline: {d['pipeline_id']}")
        print(f"  总结论数: {d['total_claims']}")
        print(f"  通过校验: {d['passed_claims']}")
        print(f"  未通过:   {d['failed_claims']}")
        print(f"  存疑:     {d['uncertain_claims']}")
        print(f"  校验轮次: {d['verification_rounds']}")
        print(f"  Agent链路: {' → '.join(t['agent'] for t in d['traces'])}")
        print(f"{'='*60}")
